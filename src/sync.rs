use std::collections::HashMap;

use anyhow::Context;
use futures::{
    stream::BoxStream,
    StreamExt,
};
use futures_async_stream::try_stream;
use serde::{
    Deserialize,
    Serialize,
};
use value_type::Inner as FivetranValue;

use crate::{
    convert::to_fivetran_row,
    convex_api::{
        DocumentDeltasCursor,
        ListSnapshotCursor,
        Source,
    },
    fivetran_sdk::{
        self,
        operation::Op,
        update_response,
        value_type,
        LogEntry,
        LogLevel,
        OpType,
        Operation,
        Record,
        UpdateResponse as FivetranUpdateResponse,
        ValueType,
    },
};

/// The value currently used for the `version` field of [`State`].
const CURSOR_VERSION: i64 = 1;

/// Stores the current synchronization state of a destination. A state will be
/// send (as JSON) to Fivetran every time we perform a checkpoint, and will be
/// returned to us every time Fivetran calls the `update` method of the
/// connector.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct State {
    /// The version of the connector that emitted this checkpoint. Could be used
    /// in the future to support backward compatibility with older state
    /// formats.
    pub version: Option<i64>,

    #[serde(default)]
    pub checkpoint: Checkpoint,
}

impl State {
    pub fn create(checkpoint: Checkpoint) -> Self {
        Self {
            version: Some(CURSOR_VERSION),
            checkpoint,
        }
    }
}

#[cfg(test)]
impl Default for State {
    fn default() -> Self {
        Self::create(Checkpoint::default())
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum Checkpoint {
    /// A checkpoint emitted during the initial synchonization.
    InitialSync {
        snapshot: Option<i64>,
        cursor: Option<ListSnapshotCursor>,
    },
    /// A checkpoint emitted after an initial synchronzation has been completed.
    DeltaUpdates { cursor: DocumentDeltasCursor },
}

impl Default for Checkpoint {
    fn default() -> Self {
        Checkpoint::InitialSync {
            snapshot: None,
            cursor: None,
        }
    }
}

#[cfg(test)]
mod state_serialization_tests {
    use proptest::prelude::*;

    use crate::sync::{
        Checkpoint,
        State,
    };

    proptest! {
        #![proptest_config(ProptestConfig {
            failure_persistence: None, ..ProptestConfig::default()
        })]
        #[test]
        fn state_json_roundtrips(value in any::<State>()) {
            let json = serde_json::to_string(&value).unwrap();
            prop_assert_eq!(value, serde_json::from_str(&json).unwrap());
        }
    }

    #[test]
    fn refuses_unknown_state_object() {
        assert!(serde_json::from_str::<State>("{\"a\": \"b\"}").is_err());
    }

    #[test]
    fn refuses_unknown_checkpoint_object() {
        assert!(serde_json::from_str::<State>(
            "{ \"version\": 1, \"snapshot\": { \"NewState\": { \"cursor\": 42 } } }"
        )
        .is_err());
    }

    #[test]
    fn deserializes_v1_initial_sync_checkpoints() {
        assert_eq!(
            serde_json::from_str::<State>(
                "{ \"version\": 1, \"checkpoint\": { \"InitialSync\": { \"snapshot\": 42, \
                 \"cursor\": \"abc123\" } } }"
            )
            .unwrap(),
            State {
                version: Some(1),
                checkpoint: Checkpoint::InitialSync {
                    snapshot: Some(42),
                    cursor: Some(String::from("abc123").into()),
                },
            },
        );
    }

    #[test]
    fn deserializes_v1_delta_update_checkpoints() {
        assert_eq!(
            serde_json::from_str::<State>(
                "{ \"version\": 1, \"checkpoint\": { \"DeltaUpdates\": { \"cursor\": 42 } } }"
            )
            .unwrap(),
            State {
                version: Some(1),
                checkpoint: Checkpoint::DeltaUpdates { cursor: 42.into() },
            },
        );
    }

    #[test]
    fn deserializes_initial_state() {
        assert!(matches!(
            serde_json::from_str::<State>("{}").unwrap(),
            State {
                version: None,
                checkpoint: Checkpoint::InitialSync {
                    snapshot: None,
                    cursor: None
                },
            },
        ));
    }
}

/// A simplification of the messages sent to Fivetran in the `update` endpoint.
pub enum UpdateMessage {
    Log(LogLevel, String),
    Update {
        schema_name: Option<String>,
        table_name: String,
        op_type: OpType,
        row: HashMap<String, FivetranValue>,
    },
    Checkpoint(State),
}

/// Conversion of the simplified update message type to the actual gRPC type.
impl From<UpdateMessage> for FivetranUpdateResponse {
    fn from(value: UpdateMessage) -> Self {
        FivetranUpdateResponse {
            response: Some(match value {
                UpdateMessage::Log(level, message) => {
                    update_response::Response::LogEntry(LogEntry {
                        level: level as i32,
                        message,
                    })
                },
                UpdateMessage::Update {
                    schema_name,
                    table_name,
                    op_type,
                    row,
                } => update_response::Response::Operation(Operation {
                    op: Some(Op::Record(Record {
                        schema_name,
                        table_name,
                        r#type: op_type as i32,
                        data: row
                            .into_iter()
                            .map(|(field_name, field_value)| {
                                (
                                    field_name,
                                    ValueType {
                                        inner: Some(field_value),
                                    },
                                )
                            })
                            .collect(),
                    })),
                }),
                UpdateMessage::Checkpoint(checkpoint) => {
                    let state_json = serde_json::to_string(&checkpoint)
                        .expect("Couldn’t serialize a checkpoint");
                    update_response::Response::Operation(Operation {
                        op: Some(Op::Checkpoint(fivetran_sdk::Checkpoint { state_json })),
                    })
                },
            }),
        }
    }
}

/// Returns the stream that the `update` endpoint emits.
pub fn sync(
    source: impl Source + 'static,
    state: State,
) -> BoxStream<'static, anyhow::Result<UpdateMessage>> {
    match state.checkpoint {
        Checkpoint::InitialSync { snapshot, cursor } => {
            initial_sync(source, snapshot, cursor).boxed()
        },
        Checkpoint::DeltaUpdates { cursor } => delta_sync(source, cursor).boxed(),
    }
}

/// Performs (or resume) an initial synchronization.
#[try_stream(ok = UpdateMessage, error = anyhow::Error)]
async fn initial_sync(
    source: impl Source,
    snapshot: Option<i64>,
    cursor: Option<ListSnapshotCursor>,
) {
    yield UpdateMessage::Log(
        LogLevel::Info,
        format!("Starting an initial sync from {source}"),
    );

    let mut snapshot: Option<i64> = snapshot;
    let mut cursor: Option<ListSnapshotCursor> = cursor;
    let mut has_more = true;

    while has_more {
        yield UpdateMessage::Checkpoint(State::create(Checkpoint::InitialSync {
            snapshot,
            cursor: cursor.clone(),
        }));

        let res = source.list_snapshot(snapshot, cursor.clone(), None).await?;

        for value in res.values {
            yield UpdateMessage::Update {
                schema_name: None,
                table_name: value.table,
                op_type: OpType::Upsert,
                row: to_fivetran_row(value.fields)?,
            };
        }

        has_more = res.has_more;
        snapshot = Some(res.snapshot);
        cursor = res.cursor.map(ListSnapshotCursor::from);
    }

    yield UpdateMessage::Checkpoint(State::create(Checkpoint::DeltaUpdates {
        cursor: DocumentDeltasCursor::from(snapshot.context("Missing snapshot from response")?),
    }));

    yield UpdateMessage::Log(LogLevel::Info, "Initial sync successful".to_string());
}

/// Synchronizes the changes that happened after an initial synchronization or
/// delta synchronization has been completed.
#[try_stream(ok = UpdateMessage, error = anyhow::Error)]
async fn delta_sync(source: impl Source, cursor: DocumentDeltasCursor) {
    yield UpdateMessage::Log(
        LogLevel::Info,
        format!("Starting to apply changes from {}", source),
    );

    let mut cursor = cursor;
    let mut has_more = true;
    while has_more {
        let response = source.document_deltas(cursor, None).await?;

        for value in response.values {
            yield UpdateMessage::Update {
                schema_name: None,
                table_name: value.table,
                op_type: if value.deleted {
                    OpType::Delete
                } else {
                    OpType::Upsert
                },
                row: to_fivetran_row(value.fields)?,
            };
        }

        cursor = DocumentDeltasCursor::from(response.cursor);
        has_more = response.has_more;

        // It is safe to take a snapshot here, because document_deltas
        // guarantees that the state given by one call is consistent.
        yield UpdateMessage::Checkpoint(State::create(Checkpoint::DeltaUpdates { cursor }));
    }

    yield UpdateMessage::Log(LogLevel::Info, "Changes applied".to_string());
}
