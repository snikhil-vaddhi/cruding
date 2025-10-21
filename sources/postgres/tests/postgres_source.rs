use std::sync::Arc;

use cruding_core::{Crudable, CrudableSource, UpdateComparingParams};
use cruding_pg_source::{
    CrudablePostgresSource, PostgresCrudableConnection, PostgresCrudableConnectionInner,
    PostgresCrudableTable,
};

use sea_orm::{
    Database, DatabaseBackend, DatabaseConnection, Iterable, Schema, Statement, TransactionTrait,
    entity::prelude::*,
    sea_query::{IntoCondition, PostgresQueryBuilder},
};
use serde::{Deserialize, Serialize};
use serial_test::serial;

// --------- config ---------

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5555/postgres".to_string())
}

// --------- entity used only in tests ---------

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "items")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub mono: i64,
    pub val: i32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

impl Crudable for Model {
    type Pkey = i32;
    type MonoField = i64;
    fn pkey(&self) -> Self::Pkey {
        self.id
    }
    fn mono_field(&self) -> Self::MonoField {
        self.mono
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl PostgresCrudableTable for Entity
where
    <Self as EntityTrait>::Model: Crudable,
    <Self as EntityTrait>::Column: Iterable + PartialEq,
{
    fn get_pkey_filter(keys: &[<Model as Crudable>::Pkey]) -> impl IntoCondition {
        Column::Id.is_in(keys.to_vec())
    }

    fn get_pkey_columns() -> Vec<Self::Column> {
        vec![Column::Id]
    }
}

// --------- helpers ---------

async fn connect_and_prepare() -> DatabaseConnection {
    let conn = Database::connect(&db_url())
        .await
        .expect("connect postgres");

    // Create table if missing
    let schema = Schema::new(DatabaseBackend::Postgres);
    let stmt = schema.create_table_from_entity(Entity);
    conn.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        stmt.to_string(PostgresQueryBuilder),
    ))
    .await
    .ok(); // ignore if already exists

    // Truncate between tests
    ensure_outbox_schema(&conn).await;

    ensure_deep_diff_fn(&conn).await;

    truncate_items_and_outbox(&conn).await;

    conn
}

fn source(
    lock_for_update: bool,
    conn: DatabaseConnection,
) -> CrudablePostgresSource<Entity, (), LiveErr> {
    CrudablePostgresSource::new(conn, lock_for_update)
}

#[derive(thiserror::Error, Debug)]
pub enum LiveErr {
    #[error(transparent)]
    Db(#[from] sea_orm::DbErr),
}

async fn ensure_outbox_schema(conn: &DatabaseConnection) {
    conn.execute_unprepared(
        r#"
        DO $$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crud_op_type') THEN
            CREATE TYPE crud_op_type AS ENUM ('C','U','D');
          END IF;
        END$$;
        "#,
    )
    .await
    .expect("create crud_op_type");

    conn.execute_unprepared(
        r#"
        CREATE TABLE IF NOT EXISTS outbox (
          id                 UUID      PRIMARY KEY,
          table_name         text      NOT NULL,
          pk                 jsonb     NOT NULL,
          crud_op            crud_op_type NOT NULL,
          meta_data          jsonb     NOT NULL,
          diff               jsonb     NOT NULL,
          entity_mono        jsonb     NOT NULL,
          commit_ts          timestamptz NOT NULL,
          published_at       timestamptz,
          publish_attempts   int       NOT NULL DEFAULT 0,
          last_publish_status text
        );
        "#,
    )
    .await
    .expect("create outbox");

    conn.execute_unprepared(
        r#"
        CREATE INDEX IF NOT EXISTS outbox_unpub_idx
          ON outbox (published_at)
          WHERE published_at IS NULL;
        "#,
    )
    .await
    .expect("create idx unpub");

    conn.execute_unprepared(
        r#"
        CREATE INDEX IF NOT EXISTS outbox_commit_idx
          ON outbox (commit_ts);
        "#,
    )
    .await
    .expect("create idx commit");

    conn.execute_unprepared(
        r#"
        CREATE INDEX IF NOT EXISTS outbox_unpub_order_idx
          ON outbox (commit_ts, id)
          WHERE published_at IS NULL;
        "#,
    )
    .await
    .expect("create idx unpub_order");
}

async fn truncate_items_and_outbox(conn: &DatabaseConnection) {
    conn.execute_unprepared("TRUNCATE TABLE items RESTART IDENTITY CASCADE;")
        .await
        .expect("truncate items");

    conn.execute_unprepared("TRUNCATE TABLE outbox;")
        .await
        .expect("truncate outbox");
}

async fn ensure_deep_diff_fn(conn: &DatabaseConnection) {
    conn.execute_unprepared(
        r#"
        CREATE OR REPLACE FUNCTION jsonb_deep_diff_only_new(new_obj jsonb, old_obj jsonb)
        RETURNS jsonb
        LANGUAGE plpgsql
        IMMUTABLE
        AS $$
        DECLARE
            result jsonb := '{}'::jsonb;
            k text;
            n_val jsonb;
            o_val jsonb;
            subdiff jsonb;
            new_type text := jsonb_typeof(new_obj);
            old_type text := jsonb_typeof(old_obj);
        BEGIN
            IF new_obj = old_obj THEN
                RETURN '{}'::jsonb;
            END IF;

            IF new_type = 'object' AND old_type = 'object' THEN
                FOR k IN
                    SELECT key FROM (
                        SELECT key FROM jsonb_object_keys(new_obj) AS j1(key)
                        UNION
                        SELECT key FROM jsonb_object_keys(old_obj) AS j2(key)
                    ) u
                LOOP
                    n_val := new_obj -> k;
                    o_val := old_obj -> k;

                    IF jsonb_typeof(n_val) = 'object' AND jsonb_typeof(o_val) = 'object' THEN
                        subdiff := jsonb_deep_diff_only_new(n_val, o_val);
                        IF subdiff <> '{}'::jsonb THEN
                            result := result || jsonb_build_object(k, subdiff);
                        END IF;

                    ELSIF jsonb_typeof(n_val) = 'array' AND jsonb_typeof(o_val) = 'array' THEN
                        IF n_val <> o_val THEN
                            result := result || jsonb_build_object(k, n_val);
                        END IF;

                    ELSE
                        IF (SELECT (n_val IS DISTINCT FROM o_val)) THEN
                            result := result || jsonb_build_object(k, n_val);
                        END IF;
                    END IF;
                END LOOP;

                RETURN COALESCE(result, '{}'::jsonb);
            END IF;

            IF new_type = 'array' AND old_type = 'array' THEN
                IF new_obj <> old_obj THEN
                    RETURN new_obj;
                ELSE
                    RETURN '{}'::jsonb;
                END IF;
            END IF;

            IF (SELECT (new_obj IS DISTINCT FROM old_obj)) THEN
                RETURN new_obj;
            ELSE
                RETURN '{}'::jsonb;
            END IF;
        END;
        $$;
        "#,
    )
    .await
    .expect("create jsonb_deep_diff_only_new()");
}

// --------- tests ---------
// Serialize tests to avoid table conflicts; remove #[serial] if you manage isolation differently.

#[tokio::test]
#[serial]
async fn create_returns_inserted_rows() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn);

    let handle = src.new_source_handle();
    let rows = vec![
        Model {
            id: 1,
            mono: 10,
            val: 111,
        },
        Model {
            id: 2,
            mono: 20,
            val: 222,
        },
    ];

    let out = CrudableSource::<Model>::create(&src, rows.clone(), handle)
        .await
        .unwrap();
    assert_eq!(out, rows);
}

#[tokio::test]
#[serial]
async fn read_filters_by_keys() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn.clone());
    let h = src.new_source_handle();

    let seed = vec![
        Model {
            id: 1,
            mono: 10,
            val: 111,
        },
        Model {
            id: 2,
            mono: 20,
            val: 222,
        },
        Model {
            id: 3,
            mono: 30,
            val: 333,
        },
    ];
    CrudableSource::<Model>::create(&src, seed, h)
        .await
        .unwrap();

    let handle = src.new_source_handle();
    let out = CrudableSource::<Model>::read(&src, &[1, 3], handle)
        .await
        .unwrap();
    assert_eq!(out.len(), 2);
    assert!(out.iter().any(|m| m.id == 1));
    assert!(out.iter().any(|m| m.id == 3));
}

#[tokio::test]
#[serial]
async fn update_many_returns_updated() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn.clone());
    let h = src.new_source_handle();

    let seed = vec![
        Model {
            id: 1,
            mono: 1,
            val: 10,
        },
        Model {
            id: 2,
            mono: 1,
            val: 20,
        },
    ];
    CrudableSource::<Model>::create(&src, seed, h)
        .await
        .unwrap();

    let updated = vec![
        Model {
            id: 1,
            mono: 2,
            val: 999,
        },
        Model {
            id: 2,
            mono: 3,
            val: 888,
        },
    ];

    let handle = src.new_source_handle();
    let keys: Vec<_> = updated.iter().map(|m| m.id).collect();
    let current = CrudableSource::<Model>::read_for_update(&src, &keys, handle)
        .await
        .unwrap();

    let handle = src.new_source_handle();
    let params = UpdateComparingParams {
        current,
        update_payload: updated.clone(),
    };
    let out = CrudableSource::<Model>::update(&src, params, handle)
        .await
        .unwrap();
    assert_eq!(out, updated);
}

#[tokio::test]
#[serial]
async fn delete_returns_deleted_rows() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn.clone());
    let h = src.new_source_handle();

    let seed = vec![
        Model {
            id: 2,
            mono: 2,
            val: 22,
        },
        Model {
            id: 4,
            mono: 4,
            val: 44,
        },
    ];
    CrudableSource::<Model>::create(&src, seed.clone(), h)
        .await
        .unwrap();

    let handle = src.new_source_handle();
    let out = CrudableSource::<Model>::delete(&src, &[2, 4], handle)
        .await
        .unwrap();
    // We expect the deleted rows to be returned (SeaORM's returning)
    assert_eq!(out.len(), 2);
    assert!(out.iter().any(|m| m.id == 2));
    assert!(out.iter().any(|m| m.id == 4));
}

#[tokio::test]
#[serial]
async fn read_for_update_owned_and_borrowed_behave() {
    // lock_for_update = true
    let conn = connect_and_prepare().await;
    let src = source(true, conn.clone());

    // Seed
    let h_seed = src.new_source_handle();
    CrudableSource::<Model>::create(
        &src,
        vec![Model {
            id: 7,
            mono: 70,
            val: 700,
        }],
        h_seed,
    )
    .await
    .unwrap();

    // Case A: plain connection -> source begins & commits internally; handle ends as Connection
    let h1 = src.new_source_handle();
    let rows1 = CrudableSource::<Model>::read_for_update(&src, &[7], h1.clone())
        .await
        .unwrap();
    assert_eq!(rows1[0].id, 7);
    match &*h1.get_conn().read().await {
        PostgresCrudableConnectionInner::Connection(_) => {}
        _ => panic!("expected Connection after auto-commit"),
    }

    // Case B: owned tx -> remains our responsibility, but your impl auto-commits in read_for_update;
    // verify handle reset to Connection
    let h2 = src.new_source_handle();
    h2.get_conn()
        .write()
        .await
        .maybe_begin_transaction()
        .await
        .unwrap();
    let rows2 = CrudableSource::<Model>::read_for_update(&src, &[7], h2.clone())
        .await
        .unwrap();
    assert_eq!(rows2[0].id, 7);
    match &*h2.get_conn().read().await {
        PostgresCrudableConnectionInner::Connection(_) => {}
        _ => panic!("expected Connection after auto-commit of owned tx"),
    }

    // Case C: borrowed tx -> must NOT be committed by the source; handle stays Borrowed
    let tx = conn.begin().await.unwrap();
    let h3 = PostgresCrudableConnection::new(PostgresCrudableConnectionInner::BorrowedTransaction(
        Arc::new(tx),
    ));
    let rows3 = CrudableSource::<Model>::read_for_update(&src, &[7], h3.clone())
        .await
        .unwrap();
    assert_eq!(rows3[0].id, 7);
    match &*h3.get_conn().read().await {
        PostgresCrudableConnectionInner::BorrowedTransaction(_) => {}
        _ => panic!("borrowed tx must remain borrowed"),
    }
}

#[tokio::test]
#[serial]
async fn use_cache_policy_is_false_inside_tx_true_otherwise() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn.clone());

    // plain connection => use cache
    let h1 = src.new_source_handle();
    assert!(CrudableSource::<Model>::should_use_cache(&src, h1).await);

    // owned transaction => bypass cache
    let h2 = src.new_source_handle();
    h2.get_conn()
        .write()
        .await
        .maybe_begin_transaction()
        .await
        .unwrap();
    assert!(!CrudableSource::<Model>::should_use_cache(&src, h2).await);

    // borrowed transaction => bypass cache
    let tx = conn.begin().await.unwrap();
    let h3 = PostgresCrudableConnection::new(PostgresCrudableConnectionInner::BorrowedTransaction(
        Arc::new(tx),
    ));
    assert!(!CrudableSource::<Model>::should_use_cache(&src, h3).await);
}

#[tokio::test]
#[serial]
async fn end_to_end_inside_owned_tx() {
    let conn = connect_and_prepare().await;
    let src = source(true, conn);

    let h = src.new_source_handle();
    h.get_conn()
        .write()
        .await
        .maybe_begin_transaction()
        .await
        .unwrap();

    // create
    let c = CrudableSource::<Model>::create(
        &src,
        vec![Model {
            id: 10,
            mono: 1,
            val: 10,
        }],
        h.clone(),
    )
    .await
    .unwrap();
    assert_eq!(c[0].id, 10);

    // read
    let r = CrudableSource::<Model>::read(&src, &[10], h.clone())
        .await
        .unwrap();
    assert_eq!(r[0].val, 10);

    let updated = vec![Model {
        id: 10,
        mono: 2,
        val: 20,
    }];

    let keys = [10];
    let current = CrudableSource::<Model>::read_for_update(&src, &keys, h.clone())
        .await
        .unwrap();

    let params = UpdateComparingParams {
        current,
        update_payload: updated.clone(),
    };

    let u = CrudableSource::<Model>::update(&src, params, h.clone())
        .await
        .unwrap();

    assert_eq!(u[0].mono, 2);

    // delete
    let d = CrudableSource::<Model>::delete(&src, &[10], h.clone())
        .await
        .unwrap();
    assert_eq!(d[0].id, 10);

    // commit (handle should reset to Connection)
    h.get_conn().write().await.maybe_commit().await.unwrap();
    match &*h.get_conn().read().await {
        PostgresCrudableConnectionInner::Connection(_) => {}
        _ => panic!("expected Connection after explicit commit"),
    }
}

#[tokio::test]
#[serial]
async fn create_emits_outbox_row() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn.clone());

    // Use a fresh handle; no explicit tx required for this test
    let h = src.new_source_handle();

    let new_item = Model {
        id: 1,
        mono: 10,
        val: 111,
    };
    let out = CrudableSource::<Model>::create(&src, vec![new_item.clone()], h)
        .await
        .expect("create must succeed");

    assert_eq!(out.len(), 1);
    assert_eq!(out[0], new_item);

    // Verify outbox
    // We check a few key columns; you can expand as needed.
    let rows = conn
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            r#"
        SELECT table_name, pk, crud_op::text AS crud_op, meta_data, diff, entity_mono, published_at, publish_attempts
        FROM outbox
        ORDER BY commit_ts, id
        "#
            .to_string(),
        ))
        .await
        .expect("select outbox");

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    let table_name: String = row.try_get("", "table_name").unwrap();
    let pk: serde_json::Value = row.try_get("", "pk").unwrap();
    let crud_op: String = row.try_get("", "crud_op").unwrap();
    let meta: serde_json::Value = row.try_get("", "meta_data").unwrap();
    let diff: serde_json::Value = row.try_get("", "diff").unwrap();
    let entity_mono: serde_json::Value = row.try_get("", "entity_mono").unwrap();
    let published_at: Option<chrono::DateTime<chrono::Utc>> =
        row.try_get("", "published_at").unwrap();
    let publish_attempts: i32 = row.try_get("", "publish_attempts").unwrap();

    assert_eq!(table_name, "items");
    assert_eq!(crud_op, "C"); // create
    assert_eq!(pk, serde_json::json!({"id": 1}));
    assert_eq!(entity_mono, serde_json::json!(10));
    assert!(meta.get("table").is_some()); // e.g., {"table":"items",...}
    assert!(diff.is_object()); // your create diff policy (often minimal or full "after")
    assert!(published_at.is_none());
    assert_eq!(publish_attempts, 0);
}

#[tokio::test]
#[serial]
async fn create_many_emits_many_outbox_rows() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn.clone());

    let h = src.new_source_handle();
    let rows = vec![
        Model {
            id: 11,
            mono: 101,
            val: 1,
        },
        Model {
            id: 12,
            mono: 102,
            val: 2,
        },
        Model {
            id: 13,
            mono: 103,
            val: 3,
        },
    ];

    CrudableSource::<Model>::create(&src, rows.clone(), h)
        .await
        .unwrap();

    let rows = conn
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            r#"
        SELECT pk, crud_op::text AS crud_op
        FROM outbox
        WHERE table_name = 'items'
        ORDER BY commit_ts, id
        "#
            .to_string(),
        ))
        .await
        .unwrap();

    assert_eq!(rows.len(), 3);
    let pks: Vec<serde_json::Value> = rows.iter().map(|r| r.try_get("", "pk").unwrap()).collect();
    let ops: Vec<String> = rows
        .iter()
        .map(|r| r.try_get("", "crud_op").unwrap())
        .collect();

    assert!(pks.contains(&serde_json::json!({"id": 11})));
    assert!(pks.contains(&serde_json::json!({"id": 12})));
    assert!(pks.contains(&serde_json::json!({"id": 13})));
    assert!(ops.iter().all(|op| op == "C"));
}

#[tokio::test]
#[serial]
async fn create_inside_owned_tx_emits_outbox_atomically() {
    let conn = connect_and_prepare().await;
    let src = source(true, conn.clone());

    let h = src.new_source_handle();
    // Start a transaction that our source "owns"
    h.get_conn()
        .write()
        .await
        .maybe_begin_transaction()
        .await
        .unwrap();

    // Do the create
    CrudableSource::<Model>::create(
        &src,
        vec![Model {
            id: 21,
            mono: 210,
            val: 900,
        }],
        h.clone(),
    )
    .await
    .unwrap();

    // Outbox row should be visible *within the same tx* if you query using the same tx.
    // We'll commit now and then verify with the connection.
    h.get_conn().write().await.maybe_commit().await.unwrap();

    // Verify outbox after commit with plain connection
    let rows = conn
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            r#"
        SELECT table_name, pk, crud_op::text AS crud_op, entity_mono
        FROM outbox
        WHERE (pk->>'id')::int = 21
        "#
            .to_string(),
        ))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    let table_name: String = row.try_get("", "table_name").unwrap();
    let pk: serde_json::Value = row.try_get("", "pk").unwrap();
    let crud_op: String = row.try_get("", "crud_op").unwrap();
    let entity_mono: serde_json::Value = row.try_get("", "entity_mono").unwrap();

    assert_eq!(table_name, "items");
    assert_eq!(crud_op, "C");
    assert_eq!(pk, serde_json::json!({"id": 21}));
    assert_eq!(entity_mono, serde_json::json!(210));
}

#[tokio::test]
#[serial]
async fn update_emits_outbox_row_with_changed_fields() {
    let conn = connect_and_prepare().await;
    let src = source(true, conn.clone());

    // seed
    CrudableSource::<Model>::create(
        &src,
        vec![Model {
            id: 1,
            mono: 1,
            val: 10,
        }],
        src.new_source_handle(),
    )
    .await
    .unwrap();

    // current snapshot (for diffing)
    let current = CrudableSource::<Model>::read_for_update(&src, &[1], src.new_source_handle())
        .await
        .unwrap();

    // apply update (both mono and val change)
    let updated = vec![Model {
        id: 1,
        mono: 2,
        val: 20,
    }];

    CrudableSource::<Model>::update(
        &src,
        UpdateComparingParams {
            current,
            update_payload: updated,
        },
        src.new_source_handle(),
    )
    .await
    .unwrap();

    // verify outbox
    let rows = conn
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            r#"
        SELECT pk, crud_op::text AS crud_op, diff
        FROM outbox
        WHERE table_name = 'items' AND (pk->>'id')::int = 1 AND crud_op::text = 'U' 
        ORDER BY commit_ts, id
        "#
            .to_string(),
        ))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let crud_op: String = rows[0].try_get("", "crud_op").unwrap();
    let pk: serde_json::Value = rows[0].try_get("", "pk").unwrap();
    let diff: serde_json::Value = rows[0].try_get("", "diff").unwrap();

    assert_eq!(crud_op, "U");
    assert_eq!(pk, serde_json::json!({"id": 1}));
    // deep diff with new values only; both fields changed
    assert_eq!(diff, serde_json::json!({"mono": 2, "val": 20}));
}

#[tokio::test]
#[serial]
async fn update_no_changes_results_empty_diff() {
    let conn = connect_and_prepare().await;
    let src = source(true, conn.clone());

    CrudableSource::<Model>::create(
        &src,
        vec![Model {
            id: 2,
            mono: 5,
            val: 50,
        }],
        src.new_source_handle(),
    )
    .await
    .unwrap();

    let current = CrudableSource::<Model>::read_for_update(&src, &[2], src.new_source_handle())
        .await
        .unwrap();

    // no changes
    let updated = vec![Model {
        id: 2,
        mono: 5,
        val: 50,
    }];

    CrudableSource::<Model>::update(
        &src,
        UpdateComparingParams {
            current,
            update_payload: updated,
        },
        src.new_source_handle(),
    )
    .await
    .unwrap();

    let rows = conn
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            r#"SELECT diff FROM outbox WHERE table_name = 'items' AND (pk->>'id')::int = 2 AND crud_op::text = 'U'"#
                .to_string(),
        ))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let diff: serde_json::Value = rows[0].try_get("", "diff").unwrap();
    assert_eq!(diff, serde_json::json!({})); // nothing changed
}

#[tokio::test]
#[serial]
async fn update_partial_change_emits_only_changed_fields() {
    let conn = connect_and_prepare().await;
    let src = source(true, conn.clone());

    CrudableSource::<Model>::create(
        &src,
        vec![Model {
            id: 3,
            mono: 10,
            val: 100,
        }],
        src.new_source_handle(),
    )
    .await
    .unwrap();

    let current = CrudableSource::<Model>::read_for_update(&src, &[3], src.new_source_handle())
        .await
        .unwrap();

    // change val only
    let updated = vec![Model {
        id: 3,
        mono: 10,
        val: 999,
    }];

    CrudableSource::<Model>::update(
        &src,
        UpdateComparingParams {
            current,
            update_payload: updated,
        },
        src.new_source_handle(),
    )
    .await
    .unwrap();

    let rows = conn
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            r#"SELECT diff FROM outbox WHERE table_name = 'items' AND (pk->>'id')::int = 3 AND crud_op::text = 'U'"#
                .to_string(),
        ))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let diff: serde_json::Value = rows[0].try_get("", "diff").unwrap();
    // Only 'val' should appear; 'mono' unchanged and omitted
    assert_eq!(diff, serde_json::json!({"val": 999}));
}

#[tokio::test]
#[serial]
async fn update_many_rows_emit_multiple_outbox_rows() {
    let conn = connect_and_prepare().await;
    let src = source(true, conn.clone());

    CrudableSource::<Model>::create(
        &src,
        vec![
            Model {
                id: 11,
                mono: 1,
                val: 10,
            },
            Model {
                id: 12,
                mono: 2,
                val: 20,
            },
        ],
        src.new_source_handle(),
    )
    .await
    .unwrap();

    let current =
        CrudableSource::<Model>::read_for_update(&src, &[11, 12], src.new_source_handle())
            .await
            .unwrap();

    // change each differently
    let updated = vec![
        Model {
            id: 11,
            mono: 3,
            val: 10,
        }, // only mono changes
        Model {
            id: 12,
            mono: 2,
            val: 99,
        }, // only val changes
    ];

    CrudableSource::<Model>::update(
        &src,
        UpdateComparingParams {
            current,
            update_payload: updated,
        },
        src.new_source_handle(),
    )
    .await
    .unwrap();

    let rows = conn
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            r#"
        SELECT (pk->>'id')::int AS id, diff
        FROM outbox
        WHERE table_name = 'items' AND (pk->>'id')::int IN (11, 12) AND crud_op::text = 'U'
        ORDER BY id
        "#
            .to_string(),
        ))
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);

    let id1: i32 = rows[0].try_get("", "id").unwrap();
    let diff1: serde_json::Value = rows[0].try_get("", "diff").unwrap();
    let id2: i32 = rows[1].try_get("", "id").unwrap();
    let diff2: serde_json::Value = rows[1].try_get("", "diff").unwrap();

    assert_eq!(id1, 11);
    assert_eq!(diff1, serde_json::json!({"mono": 3}));

    assert_eq!(id2, 12);
    assert_eq!(diff2, serde_json::json!({"val": 99}));
}

#[tokio::test]
#[serial]
async fn update_inside_owned_tx_emits_outbox_and_commits() {
    let conn = connect_and_prepare().await;
    let src = source(true, conn.clone());

    // start an owned tx on the handle
    let h = src.new_source_handle();
    h.get_conn()
        .write()
        .await
        .maybe_begin_transaction()
        .await
        .unwrap();

    // seed inside that tx
    CrudableSource::<Model>::create(
        &src,
        vec![Model {
            id: 30,
            mono: 1,
            val: 10,
        }],
        h.clone(),
    )
    .await
    .unwrap();

    // current inside same tx
    let current = CrudableSource::<Model>::read_for_update(&src, &[30], h.clone())
        .await
        .unwrap();

    // update inside same tx
    let updated = vec![Model {
        id: 30,
        mono: 2,
        val: 10,
    }];

    CrudableSource::<Model>::update(
        &src,
        UpdateComparingParams {
            current,
            update_payload: updated,
        },
        h.clone(),
    )
    .await
    .unwrap();

    // commit
    h.get_conn().write().await.maybe_commit().await.unwrap();

    // verify outbox visible after commit
    let rows = conn
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            r#"SELECT crud_op::text AS crud_op, diff FROM outbox WHERE (pk->>'id')::int = 30 AND crud_op::text = 'U'"#
                .to_string(),
        ))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let crud_op: String = rows[0].try_get("", "crud_op").unwrap();
    let diff: serde_json::Value = rows[0].try_get("", "diff").unwrap();

    assert_eq!(crud_op, "U");
    assert_eq!(diff, serde_json::json!({"mono": 2}));
}

#[tokio::test]
#[serial]
async fn delete_emits_outbox_with_old_snapshot() {
    let conn = connect_and_prepare().await;
    let src = source(true, conn.clone());

    // Seed
    CrudableSource::<Model>::create(
        &src,
        vec![Model {
            id: 41,
            mono: 7,
            val: 70,
        }],
        src.new_source_handle(),
    )
    .await
    .unwrap();

    // Delete
    CrudableSource::<Model>::delete(&src, &[41], src.new_source_handle())
        .await
        .unwrap();

    // Verify outbox D
    let rows = conn
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            r#"
        SELECT crud_op::text AS crud_op, pk, diff, entity_mono
        FROM outbox
        WHERE table_name = 'items' AND (pk->>'id')::int = 41 AND crud_op::text = 'D'
        "#
            .to_string(),
        ))
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let crud_op: String = rows[0].try_get("", "crud_op").unwrap();
    let pk: serde_json::Value = rows[0].try_get("", "pk").unwrap();
    let diff: serde_json::Value = rows[0].try_get("", "diff").unwrap();
    let mono: serde_json::Value = rows[0].try_get("", "entity_mono").unwrap();

    assert_eq!(crud_op, "D");
    assert_eq!(pk, serde_json::json!({"id": 41}));
    assert_eq!(mono, serde_json::json!(7));
    // Full old snapshot:
    assert_eq!(diff, serde_json::json!({ "id": 41, "mono": 7, "val": 70 }));
}

#[tokio::test]
#[serial]
async fn delete_nonexistent_keys_emits_nothing() {
    let conn = connect_and_prepare().await;
    let src = source(true, conn.clone());

    // Ensure empty
    CrudableSource::<Model>::delete(&src, &[9999], src.new_source_handle())
        .await
        .unwrap();

    let rows = conn
        .query_all(Statement::from_string(
            DatabaseBackend::Postgres,
            r#"SELECT 1 FROM outbox WHERE (pk->>'id')::int = 9999 AND crud_op::text = 'D'"#
                .to_string(),
        ))
        .await
        .unwrap();

    assert!(rows.is_empty());
}
