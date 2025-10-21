use sea_orm::EntityTrait;
use serde_json::json;

pub fn pk_json_from_model<M: EntityTrait>(m: &M::Model) -> serde_json::Value {
    json!({ "id": m.pkey() })
}

pub fn meta_from_model<M: EntityTrait>(_m: &M::Model) -> serde_json::Value {
    json!({})
}
