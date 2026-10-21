use crate::{
    errors::{AppError, AppErrorWithMessage},
    state::AppState,
    types::outbox,
};
use sqlx::{query, Row};
use uuid::Uuid;

pub async fn create_outbox_event(
    state: &AppState,
    payload: outbox::OutboxEventPayload,
) -> Result<(), AppErrorWithMessage> {
    let event_id = Uuid::new_v4();

    query(
        r#"
        INSERT INTO outbox_events (
            id,
            aggregate_id,
            aggregate_type,
            event_type,
            payload
        )
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(event_id)
    .bind(&payload.aggregate_id)
    .bind(&payload.aggregate_type)
    .bind(&payload.event_type)
    .bind(&payload.payload)
    .execute(&state.db)
    .await
    .map_err(|e: sqlx::Error| {
        eprintln!("Failed to create outbox event {event_id}: {e:?}");
        AppError::Internal.with_message("Unable to queue event")
    })?;

    Ok(())
}


pub async fn check_for_events(state: &AppState) -> Result<Vec<outbox::OutboxEventPayload>, AppErrorWithMessage> {
    let events = query(
        r#"
        SELECT
            aggregate_type,
            aggregate_id,
            event_type,
            payload
        FROM outbox_events
        "#,
    )
    .fetch_all(&state.db)
    .await
    .map_err(|e| {
        eprintln!("Failed to fetch outbox events: {e:?}");
        AppError::Internal.with_message("Unable to fetch events")
    })?;

    events
        .into_iter()
        .map(|event| {
            Ok(outbox::OutboxEventPayload {
                aggregate_type: event.try_get("aggregate_type").map_err(|e| {
                    eprintln!("Failed to decode aggregate type: {e:?}");
                    AppError::Internal.with_message("Unable to fetch events")
                })?,
                aggregate_id: event.try_get("aggregate_id").map_err(|e| {
                    eprintln!("Failed to decode aggregate id: {e:?}");
                    AppError::Internal.with_message("Unable to fetch events")
                })?,
                event_type: event.try_get("event_type").map_err(|e| {
                    eprintln!("Failed to decode event type: {e:?}");
                    AppError::Internal.with_message("Unable to fetch events")
                })?,
                payload: event.try_get("payload").map_err(|e| {
                    eprintln!("Failed to decode event payload: {e:?}");
                    AppError::Internal.with_message("Unable to fetch events")
                })?,
            })
        })
        .collect()
}