use chrono::Utc;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement, TransactionTrait};
use serde_json::Value as JsonValue;

pub async fn run_outbox_worker(db: DatabaseConnection) -> anyhow::Result<()> {
    loop {
        let rows = db
            .query_all(Statement::from_string(
                DbBackend::Postgres,
                r#"
            SELECT id, table_name, pk, crud_op, meta_data, diff, entity_mono
            FROM outbox
            WHERE published_at IS NULL
            ORDER BY commit_ts, id
            LIMIT 100
            FOR UPDATE SKIP LOCKED
            "#
                .to_owned(),
            ))
            .await?;

        if rows.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }

        let tx = db.begin().await?;
        for row in rows {
            let id: uuid::Uuid = row.try_get("", "id")?;
            let table: String = row.try_get("", "table_name")?;
            let meta: JsonValue = row.try_get("", "meta_data")?;
            let diff: JsonValue = row.try_get("", "diff")?;

            let topic = classify_topic(&table, &meta);

            let ok = publish(&topic, &diff).await.is_ok();

            if ok {
                db.execute(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "UPDATE outbox
                     SET published_at = $1, publish_attempts = publish_attempts + 1, last_publish_status = 'ok'
                     WHERE id = $2",
                    vec![Utc::now().into(), id.into()],
                )).await?;
            } else {
                db.execute(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "UPDATE outbox
                     SET publish_attempts = publish_attempts + 1, last_publish_status = 'error'
                     WHERE id = $1",
                    vec![id.into()],
                ))
                .await?;
            }
        }
        tx.commit().await?;
    }
}

fn classify_topic(_table: &str, _meta: &JsonValue) -> String {
    todo!();
}

async fn publish(_topic: &str, _payload: &JsonValue) -> anyhow::Result<()> {
    todo!();
}
