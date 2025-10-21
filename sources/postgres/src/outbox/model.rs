use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "outbox")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: uuid::Uuid,
    pub table_name: String,
    pub pk: Json,
    pub crud_op: CrudOpType,
    pub meta_data: Json,
    pub diff: Json,
    pub entity_mono: Json,
    pub commit_ts: DateTimeWithTimeZone,
    pub published_at: Option<DateTimeWithTimeZone>,
    pub publish_attempts: i32,
    pub last_publish_status: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Enum", enum_name = "crud_op_type")]
pub enum CrudOpType {
    #[sea_orm(string_value = "C")]
    C,
    #[sea_orm(string_value = "U")]
    U,
    #[sea_orm(string_value = "D")]
    D,
}
