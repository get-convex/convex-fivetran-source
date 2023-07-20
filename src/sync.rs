use std::{
    collections::HashMap,
    fmt::Display,
};

use anyhow::Context;
use async_trait::async_trait;
use futures::{
    stream::BoxStream,
    StreamExt,
};
use futures_async_stream::try_stream;
use schemars::schema::Schema;
use serde::{
    Deserialize,
    Serialize,
};
use value_type::Inner as FivetranValue;

use crate::{
    convert::to_fivetran_row,
    convex_api::{
        Cursor,
        DatabaseSchema,
        DocumentDeltasResponse,
        FieldName,
        ListSnapshotResponse,
        TableName,
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

const CURSOR_VERSION: i64 = 1;

#[async_trait]
pub trait Source: Display + Send {
    async fn json_schemas(&self) -> anyhow::Result<DatabaseSchema>;

    async fn list_snapshot(
        &self,
        snapshot: Option<i64>,
        cursor: Option<String>,
        table_name: Option<String>,
    ) -> anyhow::Result<ListSnapshotResponse>;

    async fn document_deltas(
        &self,
        cursor: Cursor,
        table_name: Option<String>,
    ) -> anyhow::Result<DocumentDeltasResponse>;

    async fn get_columns(&self) -> anyhow::Result<HashMap<TableName, Vec<FieldName>>> {
        let schema = self.json_schemas().await?;

        schema
            .0
            .into_iter()
            .map(|(table_name, table_schema)| {
                let system_columns = ["_id", "_creationTime"].into_iter().map(String::from);
                let user_columns = match table_schema {
                    Schema::Bool(_) => vec![], // Empty table
                    Schema::Object(schema) => schema
                        .object
                        .context("Unexpected non-object validator for a document")?
                        .properties
                        .into_keys()
                        .filter(|key| !key.starts_with('_'))
                        .collect(),
                };

                let columns = system_columns
                    .chain(user_columns.into_iter())
                    .map(FieldName)
                    .collect();

                Ok((table_name, columns))
            })
            .try_collect()
    }
}

pub enum UpdateMessage {
    Log(LogLevel, String),
    Update {
        schema_name: Option<String>,
        table_name: String,
        op_type: OpType,
        row: HashMap<String, FivetranValue>,
    },
    Checkpoint(Checkpoint),
}

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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(untagged, deny_unknown_fields)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum State {
    Checkpoint(Checkpoint),
    InitialSync {},
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Checkpoint {
    version: i64,
    cursor: Cursor,
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
            assert_eq!(value, serde_json::from_str(&json).unwrap());
        }
    }

    #[test]
    fn refuses_unknown_json_object() {
        assert!(serde_json::from_str::<State>("{\"a\": \"b\"}").is_err());
    }

    #[test]
    fn deserializes_v1_checkpoints() {
        assert_eq!(
            serde_json::from_str::<State>("{\"version\": 1, \"cursor\": 42}").unwrap(),
            State::Checkpoint(Checkpoint {
                version: 1,
                cursor: 42.into(),
            }),
        );
    }

    #[test]
    fn deserializes_initial_state() {
        assert!(matches!(
            serde_json::from_str::<State>("{}").unwrap(),
            State::InitialSync {}
        ));
    }
}

impl Checkpoint {
    pub fn create(cursor: Cursor) -> Self {
        Self {
            version: CURSOR_VERSION,
            cursor,
        }
    }
}

pub fn sync(
    source: impl Source + 'static,
    state: State,
) -> BoxStream<'static, anyhow::Result<UpdateMessage>> {
    match state {
        State::InitialSync {} => initial_sync(source).boxed(),
        State::Checkpoint(checkpoint) => delta_sync(source, checkpoint).boxed(),
    }
}

#[try_stream(ok = UpdateMessage, error = anyhow::Error)]
pub async fn initial_sync(source: impl Source) {
    yield UpdateMessage::Log(
        LogLevel::Info,
        format!("Starting an initial sync from {source}"),
    );

    let mut snapshot: Option<i64> = None;
    let mut cursor: Option<String> = None;
    let mut has_more = true;

    while has_more {
        let res = source.list_snapshot(snapshot, cursor, None).await?;

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
        cursor = res.cursor;
    }

    yield UpdateMessage::Checkpoint(Checkpoint::create(Cursor::from(snapshot.unwrap())));

    yield UpdateMessage::Log(LogLevel::Info, "Initial sync successful".to_string());
}

#[try_stream(ok = UpdateMessage, error = anyhow::Error)]
pub async fn delta_sync(source: impl Source, checkpoint: Checkpoint) {
    yield UpdateMessage::Log(
        LogLevel::Info,
        format!("Starting to apply changes from {}", source),
    );

    let mut cursor = checkpoint.cursor;
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

        cursor = Cursor::from(response.cursor);
        has_more = response.has_more;

        // It is safe to take a snapshot here, because document_deltas guarantees that
        // the state given by one call is consistent.
        yield UpdateMessage::Checkpoint(Checkpoint::create(cursor));
    }

    yield UpdateMessage::Log(LogLevel::Info, "Changes applied".to_string());
}
