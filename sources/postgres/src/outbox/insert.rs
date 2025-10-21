use chrono::{DateTime, Utc};
use sea_orm::{ActiveModelTrait, DatabaseTransaction, Set};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use super::model::{ActiveModel as OutboxAM, CrudOpType};

pub struct Outbox;

impl Outbox {
    #[allow(clippy::too_many_arguments)]
    pub async fn insert(
        txn: &DatabaseTransaction,
        table_name: &str,
        pk_json: JsonValue,
        crud_op: CrudOpType,
        meta_data: JsonValue,
        diff: JsonValue,
        entity_mono: JsonValue,
        commit_ts: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let am = OutboxAM {
            id: Set(Uuid::new_v4()),
            table_name: Set(table_name.to_string()),
            pk: Set(pk_json.into()),
            crud_op: Set(crud_op),
            meta_data: Set(meta_data.into()),
            diff: Set(diff.into()),
            entity_mono: Set(entity_mono.into()),
            commit_ts: Set(commit_ts.into()),
            published_at: Set(None),
            publish_attempts: Set(0),
            last_publish_status: Set(None),
        };
        am.insert(txn).await?;
        Ok(())
    }
}
