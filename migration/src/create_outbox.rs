use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();

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
        .await?;

        conn.execute_unprepared(
            r#"CREATE TABLE IF NOT EXISTS outbox (
                id                  UUID      PRIMARY KEY,
                table_name          text      NOT NULL,
                pk                  jsonb     NOT NULL,
                crud_op             crud_op_type NOT NULL,
                meta_data           jsonb     NOT NULL,
                diff                jsonb     NOT NULL,
                entity_mono         jsonb     NOT NULL,
                commit_ts           timestamptz NOT NULL,
                published_at        timestamptz,
                publish_attempts    int       NOT NULL DEFAULT 0,
                last_publish_status text
            );"#,
        )
        .await?;

        conn.execute_unprepared(
            r#"CREATE INDEX IF NOT EXISTS outbox_unpub_order_idx
               ON outbox (commit_ts, id)
               WHERE published_at IS NULL;"#,
        )
        .await?;

        conn.execute_unprepared(
            r#"CREATE INDEX IF NOT EXISTS outbox_commit_idx
               ON outbox (commit_ts);"#,
        )
        .await?;

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
                kn RECORD;
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
                            SELECT key FROM jsonb_object_keys(new_obj)
                            UNION
                            SELECT key FROM jsonb_object_keys(old_obj)
                        ) AS u
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
            ;"#,
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();
        conn.execute_unprepared("DROP TABLE IF EXISTS outbox")
            .await?;
        conn.execute_unprepared("DROP FUNCTION IF EXISTS jsonb_deep_diff_only_new(jsonb, jsonb);")
            .await?;
        conn.execute_unprepared(
            r#"
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crud_op_type') THEN
            DROP TYPE crud_op_type;
          END IF;
        END$$;
    "#,
        )
        .await?;
        Ok(())
    }
}
