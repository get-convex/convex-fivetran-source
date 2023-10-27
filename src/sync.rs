use std::collections::{
    HashMap,
    HashSet,
};

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
    log,
};

/// The value currently used for the `version` field of [`State`].
const CURSOR_VERSION: i64 = 1;

/// Stores the current synchronization state of a destination. A state will be
/// send (as JSON) to Fivetran every time we perform a checkpoint, and will be
/// returned to us every time Fivetran calls the `update` method of the
/// connector.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct State {
    /// The version of the connector that emitted this checkpoint. Could be used
    /// in the future to support backward compatibility with older state
    /// formats.
    pub version: i64,

    pub checkpoint: Checkpoint,

    /// If set, then we are tracking the full set of tables that the connector
    /// has every seen, so we are able to issue truncates the first time we
    /// see a table.
    ///
    /// Older versions of state.json do not have this field set. Once all
    /// state.json have this field, we can make this non-optional.
    pub tables_seen: Option<HashSet<String>>,
}

impl State {
    pub fn create(checkpoint: Checkpoint, tables_seen: Option<HashSet<String>>) -> Self {
        Self {
            version: CURSOR_VERSION,
            checkpoint,
            tables_seen,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(deny_unknown_fields)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum Checkpoint {
    /// A checkpoint emitted during the initial synchonization.
    InitialSync {
        snapshot: i64,
        cursor: ListSnapshotCursor,
    },
    /// A checkpoint emitted after an initial synchronzation has been completed.
    DeltaUpdates { cursor: DocumentDeltasCursor },
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
                version: 1,
                checkpoint: Checkpoint::InitialSync {
                    snapshot: 42,
                    cursor: String::from("abc123").into(),
                },
                tables_seen: None,
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
                version: 1,
                checkpoint: Checkpoint::DeltaUpdates { cursor: 42.into() },
                tables_seen: None,
            },
        );
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
    state: Option<State>,
) -> BoxStream<'static, anyhow::Result<UpdateMessage>> {
    let Some(state) = state else {
        return initial_sync(source, None, Some(HashSet::new())).boxed();
    };

    let State {
        version: _version,
        checkpoint,
        tables_seen,
    } = state;
    match checkpoint {
        Checkpoint::InitialSync { snapshot, cursor } => {
            initial_sync(source, Some((snapshot, cursor)), tables_seen).boxed()
        },
        Checkpoint::DeltaUpdates { cursor } => delta_sync(source, cursor, tables_seen).boxed(),
    }
}

/// Performs (or resume) an initial synchronization.
#[try_stream(ok = UpdateMessage, error = anyhow::Error)]
async fn initial_sync(
    source: impl Source,
    mut checkpoint: Option<(i64, ListSnapshotCursor)>,
    mut tables_seen: Option<HashSet<String>>,
) {
    let log_msg = if let Some((snapshot, _)) = checkpoint {
        format!("Resuming an initial sync from {source} at {snapshot}")
    } else {
        format!("Starting an initial sync from {source}")
    };
    log(&log_msg);
    yield UpdateMessage::Log(LogLevel::Info, log_msg);

    let mut has_more = true;

    while has_more {
        let snapshot = checkpoint.as_ref().map(|c| c.0);
        let cursor = checkpoint.as_ref().map(|c| c.1.clone());
        let res = source.list_snapshot(snapshot, cursor.clone(), None).await?;

        for value in res.values {
            if let Some(ref mut tables_seen) = tables_seen {
                // Issue truncates if we see a table for the first time.
                // Skip the behavior for legacy state.json - where tables_seen wasn't tracked.
                if !tables_seen.contains(&value.table) {
                    tables_seen.insert(value.table.clone());
                    yield UpdateMessage::Update {
                        schema_name: None,
                        table_name: value.table.clone(),
                        op_type: OpType::Truncate,
                        row: HashMap::new(),
                    };
                }
            }
            yield UpdateMessage::Update {
                schema_name: None,
                table_name: value.table,
                op_type: OpType::Upsert,
                row: to_fivetran_row(value.fields)?,
            };
        }

        has_more = res.has_more;
        if has_more {
            let cursor = ListSnapshotCursor::from(
                res.cursor.context("Missing cursor when has_more was set")?,
            );
            yield UpdateMessage::Checkpoint(State::create(
                Checkpoint::InitialSync {
                    snapshot: res.snapshot,
                    cursor: cursor.clone(),
                },
                tables_seen.clone(),
            ));
            checkpoint = Some((res.snapshot, cursor));
        }
    }

    let (snapshot, _) = checkpoint.context("list_snapshot lacking a snapshot for checkpoint")?;
    let cursor = DocumentDeltasCursor::from(snapshot);
    yield UpdateMessage::Checkpoint(State::create(
        Checkpoint::DeltaUpdates { cursor },
        tables_seen,
    ));

    yield UpdateMessage::Log(LogLevel::Info, "Initial sync successful".to_string());
    log(&format!(
        "Initial sync from {source} successful at cursor {cursor}."
    ));
}

/// Synchronizes the changes that happened after an initial synchronization or
/// delta synchronization has been completed.
#[try_stream(ok = UpdateMessage, error = anyhow::Error)]
async fn delta_sync(
    source: impl Source,
    cursor: DocumentDeltasCursor,
    mut tables_seen: Option<HashSet<String>>,
) {
    yield UpdateMessage::Log(
        LogLevel::Info,
        format!("Starting to apply changes from {source} starting at {cursor}"),
    );
    log(&format!("Delta sync from {source} starting at {cursor}."));

    let mut cursor = cursor;
    let mut has_more = true;
    while has_more {
        let response = source.document_deltas(cursor, None).await?;

        for value in response.values {
            if let Some(ref mut tables_seen) = tables_seen {
                // Issue truncates if we see a table for the first time.
                // Skip the behavior for legacy state.json - where tables_seen wasn't tracked.
                if !tables_seen.contains(&value.table) {
                    tables_seen.insert(value.table.clone());
                    yield UpdateMessage::Update {
                        schema_name: None,
                        table_name: value.table.clone(),
                        op_type: OpType::Truncate,
                        row: HashMap::new(),
                    };
                }
            }

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
        yield UpdateMessage::Checkpoint(State::create(
            Checkpoint::DeltaUpdates { cursor },
            tables_seen.clone(),
        ));
    }

    yield UpdateMessage::Log(LogLevel::Info, "Changes applied".to_string());
    log(&format!(
        "Delta sync changes applied from {source}. Final cursor {cursor}"
    ));
}
