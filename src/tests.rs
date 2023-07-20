use std::{
    collections::HashMap,
    fmt::Display,
    panic,
    vec,
};

use anyhow::Ok;
use async_trait::async_trait;
use futures::{
    Stream,
    StreamExt,
};
use maplit::hashmap;
use schemars::schema::Schema;
use serde_json::{
    json,
    Value as JsonValue,
};
use uuid::Uuid;
use value_type::Inner as FivetranValue;

use crate::{
    convex_api::{
        Cursor,
        DatabaseSchema,
        DocumentDeltasResponse,
        ListSnapshotResponse,
        SnapshotValue,
        TableName,
    },
    fivetran_sdk::{
        value_type,
        LogLevel,
        OpType,
    },
    sync::{
        delta_sync,
        initial_sync,
        Checkpoint,
        Source,
        UpdateMessage,
    },
};

type JsonDocument = HashMap<String, JsonValue>;

#[derive(Debug, Clone)]
struct FakeSource {
    tables: HashMap<String, Vec<JsonDocument>>,
    changelog: Vec<SnapshotValue>,
}

impl Default for FakeSource {
    fn default() -> Self {
        FakeSource {
            tables: hashmap! {},
            changelog: vec![],
        }
    }
}

impl FakeSource {
    fn seeded() -> Self {
        let mut source = Self::default();
        for table_name in ["table1", "table2", "table3"] {
            for i in 0..25 {
                source.insert(
                    table_name,
                    hashmap! {
                        "name".to_string() => json!(format!("Document {} of {}", i, table_name)),
                        "index".to_string() => json!(i),
                    },
                )
            }
        }

        source
    }

    fn insert(&mut self, table_name: &str, mut value: JsonDocument) {
        if value.contains_key("_id") {
            panic!("ID specified while inserting a new row");
        }
        value.insert(
            "_id".to_string(),
            JsonValue::String(Uuid::new_v4().to_string()),
        );
        value.insert("_creationTime".to_string(), json!(0));

        self.tables
            .entry(table_name.to_string())
            .or_insert_with(Vec::new)
            .push(value.clone().into_iter().collect());

        self.changelog.push(SnapshotValue {
            table: table_name.to_string(),
            deleted: false,
            fields: value,
        });
    }

    fn patch(&mut self, table_name: &str, index: usize, changed_fields: JsonValue) {
        let table = self.tables.get_mut(table_name).unwrap();
        let element = table.get_mut(index).unwrap();
        for (key, value) in changed_fields.as_object().unwrap().iter() {
            if key.starts_with('_') {
                panic!("Trying to set a system field");
            }

            element.insert(key.clone(), value.clone());
        }

        self.changelog.push(SnapshotValue {
            table: table_name.to_string(),
            deleted: false,
            fields: element.clone(),
        });
    }

    fn delete(&mut self, table_name: &str, index: usize) {
        let table = self.tables.get_mut(table_name).unwrap();
        let id = table
            .get(index)
            .unwrap()
            .get("_id")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        table.remove(index);
        self.changelog.push(SnapshotValue {
            table: table_name.to_string(),
            deleted: true,
            fields: hashmap! { "_id".to_string() => json!(id) },
        })
    }
}

impl Display for FakeSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fake_source")
    }
}

#[async_trait]
impl Source for FakeSource {
    async fn json_schemas(&self) -> anyhow::Result<DatabaseSchema> {
        Ok(DatabaseSchema(
            self.tables
                .iter()
                .map(|(table_name, rows)| {
                    let field_names = rows.iter().flat_map(|row| {
                        row.keys()
                    });

                    let schema: Schema = serde_json::from_value(json!({
                        "type": "object",
                        "additionalProperties": false,
                        "required": vec!["_id", "_creationTime"],
                        "$schema": "http://json-schema.org/draft-07/schema#",
                        "properties": JsonValue::Object(field_names.map(|field_name| (field_name.clone(), match field_name.as_ref() {
                            "_id" => json!({
                                "$description": format!("Id({})", table_name),
                                "type": "string",
                            }),
                            "_creationTime" => json!({
                                "type": "number",
                            }),
                            _ => json!({
                                // The specific type won’t be used by the connector
                                "type": "string",
                            }),
                        })).collect()),
                    })).unwrap();
                    (TableName(table_name.clone()), schema)
                })
                .collect(),
        ))
    }

    async fn list_snapshot(
        &self,
        snapshot: Option<i64>,
        cursor: Option<String>,
        table_name: Option<String>,
    ) -> anyhow::Result<ListSnapshotResponse> {
        if table_name.is_some() {
            panic!("Query by table is not supported by the fake");
        }

        if snapshot.is_some() && snapshot != Some(self.changelog.len() as i64) {
            panic!("Unexpected snapshot value");
        }

        let cursor = cursor.map(|c| c.parse().unwrap()).unwrap_or(0);
        let values_per_call = 10;
        let values: Vec<SnapshotValue> = self
            .tables
            .iter()
            .flat_map(|(table, docs)| {
                docs.iter()
                    .map(|fields| SnapshotValue {
                        table: table.to_string(),
                        deleted: false,
                        fields: fields.clone(),
                    })
                    .collect::<Vec<_>>()
            })
            .skip(cursor * values_per_call)
            .take(values_per_call)
            .collect();

        Ok(ListSnapshotResponse {
            has_more: values.len() == values_per_call,
            values,
            snapshot: self.changelog.len() as i64,
            cursor: Some((cursor + 1).to_string()),
        })
    }

    async fn document_deltas(
        &self,
        cursor: Cursor,
        table_name: Option<String>,
    ) -> anyhow::Result<DocumentDeltasResponse> {
        if table_name.is_some() {
            panic!("Per-table log not supported in fake");
        }

        let results_per_page = 5;
        let values: Vec<SnapshotValue> = self
            .changelog
            .iter()
            .skip(i64::from(cursor) as usize)
            .take(results_per_page as usize)
            .cloned()
            .collect();
        let values_len = values.len() as i64;

        Ok(DocumentDeltasResponse {
            values,
            cursor: i64::from(cursor) + values_len,
            has_more: values_len == results_per_page,
        })
    }
}

#[derive(Debug, PartialEq)]
struct FakeDestination {
    logs: Vec<(LogLevel, String)>,
    tables: HashMap<String, Vec<HashMap<String, FivetranValue>>>,
    checkpoint: Option<Checkpoint>,
}

impl Default for FakeDestination {
    fn default() -> Self {
        Self {
            logs: vec![],
            tables: hashmap![],
            checkpoint: None,
        }
    }
}

impl FakeDestination {
    fn has_log(&self, substring: &str) -> bool {
        self.logs
            .iter()
            .any(|(_, message)| message.contains(substring))
    }

    fn latest_checkpoint(&self) -> Option<Checkpoint> {
        self.checkpoint.clone()
    }

    async fn receive(&mut self, stream: impl Stream<Item = anyhow::Result<UpdateMessage>>) {
        let mut stream = Box::pin(stream);

        while let (Some(result), new_stream) = stream.into_future().await {
            stream = new_stream;

            match result.expect("Unexpected error received") {
                UpdateMessage::Log(level, message) => {
                    println!("[{:?}] {}", level, message);
                    self.logs.push((level, message));
                },
                UpdateMessage::Update {
                    schema_name,
                    table_name,
                    op_type,
                    row,
                } => {
                    if schema_name.is_some() {
                        panic!("Schemas not supported by the fake");
                    }

                    if !self.tables.contains_key(&table_name) {
                        self.tables.insert(table_name.clone(), vec![]);
                    }

                    let table = self
                        .tables
                        .get_mut(&table_name)
                        .expect("Unknown table name");
                    let id = row.get("_id").unwrap();
                    let position = table.iter().position(|row| row.get("_id").unwrap() == id);

                    match op_type {
                        OpType::Upsert => {
                            match position {
                                Some(index) => table[index] = row,
                                None => table.push(row),
                            };
                        },
                        OpType::Delete => {
                            table.remove(position.expect("Could not find the row to delete"));
                        },
                        _ => panic!("Operation not supported by the fake"),
                    };
                },
                UpdateMessage::Checkpoint(checkpoint) => {
                    self.checkpoint = Some(checkpoint);
                },
            }
        }
    }
}

#[tokio::test]
async fn initial_sync_copies_documents_from_source_to_destination() -> anyhow::Result<()> {
    let source = FakeSource::seeded();
    let mut destination = FakeDestination::default();

    destination.receive(initial_sync(source.clone())).await;

    assert!(destination.has_log("Initial sync successful"));

    assert_eq!(source.tables.len(), destination.tables.len());
    assert_eq!(
        source.tables.get("table1").unwrap().len(),
        destination.tables.get("table1").unwrap().len(),
    );

    assert_eq!(
        destination
            .tables
            .get("table1")
            .unwrap()
            .first()
            .unwrap()
            .get("name")
            .unwrap(),
        &FivetranValue::String("Document 0 of table1".to_string())
    );

    assert_eq!(
        destination
            .tables
            .get("table1")
            .unwrap()
            .get(21)
            .unwrap()
            .get("name")
            .unwrap(),
        &FivetranValue::String("Document 21 of table1".to_string())
    );

    Ok(())
}

/// Verifies that the source and the destination are in sync by starting a new
/// initial sync and verifying that the destinations match.
async fn assert_in_sync(source: impl Source, destination: &FakeDestination) {
    let mut new_sync = FakeDestination::default();
    new_sync.receive(initial_sync(source)).await;
    assert_eq!(destination.tables, new_sync.tables);
}

async fn assert_not_in_sync(source: impl Source, destination: &FakeDestination) {
    let mut new_sync = FakeDestination::default();
    new_sync.receive(initial_sync(source)).await;
    assert_ne!(destination.tables, new_sync.tables);
}

#[tokio::test]
async fn initial_sync_synchronizes_the_destination_with_the_source() -> anyhow::Result<()> {
    let source = FakeSource::seeded();
    let mut destination = FakeDestination::default();

    assert_not_in_sync(source.clone(), &destination).await;

    destination.receive(initial_sync(source.clone())).await;

    assert_in_sync(source, &destination).await;

    Ok(())
}

#[tokio::test]
async fn sync_after_adding_a_document() -> anyhow::Result<()> {
    let mut source = FakeSource::seeded();
    let mut destination = FakeDestination::default();

    destination.receive(initial_sync(source.clone())).await;
    let checkpoint = destination.latest_checkpoint().unwrap();

    source.insert(
        "table1",
        hashmap! {
            "name".to_string() => json!("New document"),
        },
    );
    destination
        .receive(delta_sync(source.clone(), checkpoint))
        .await;
    assert_in_sync(source, &destination).await;

    Ok(())
}

#[tokio::test]
async fn sync_after_modifying_a_document() -> anyhow::Result<()> {
    let mut source = FakeSource::seeded();
    let mut destination = FakeDestination::default();

    destination.receive(initial_sync(source.clone())).await;
    let checkpoint = destination.latest_checkpoint().unwrap();

    source.patch(
        "table1",
        13,
        json!({
            "name": "New name",
        }),
    );
    destination
        .receive(delta_sync(source.clone(), checkpoint))
        .await;
    assert_in_sync(source, &destination).await;

    Ok(())
}

#[tokio::test]
async fn sync_after_deleting_a_document() -> anyhow::Result<()> {
    let mut source = FakeSource::seeded();
    let mut destination = FakeDestination::default();

    destination.receive(initial_sync(source.clone())).await;
    let checkpoint = destination.latest_checkpoint().unwrap();

    source.delete("table1", 8);
    destination
        .receive(delta_sync(source.clone(), checkpoint))
        .await;
    assert_in_sync(source, &destination).await;

    Ok(())
}
