use serde_json::Value;
use uuid::Uuid;

#[derive(Debug)]
pub struct OutboxEventPayload {
    pub aggregate_type: String,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub payload: Value,
}
