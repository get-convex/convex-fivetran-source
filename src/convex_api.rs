use std::{
    collections::HashMap,
    fmt::Display,
    sync::LazyLock,
};

use anyhow::Context;
use async_trait::async_trait;
use derive_more::{
    Display,
    From,
    Into,
};
use maplit::hashmap;
use schemars::schema::Schema;
use serde::{
    de::DeserializeOwned,
    Deserialize,
    Serialize,
};
use serde_json::Value as JsonValue;
use tonic::codegen::http::{
    HeaderName,
    HeaderValue,
};

use crate::config::Config;

#[allow(clippy::declare_interior_mutable_const)]
const CONVEX_CLIENT_HEADER: HeaderName = HeaderName::from_static("convex-client");

static CONVEX_CLIENT_HEADER_VALUE: LazyLock<HeaderValue> = LazyLock::new(|| {
    let connector_version = env!("CARGO_PKG_VERSION");
    HeaderValue::from_str(&format!("fivetran-export-{connector_version}")).unwrap()
});

/// The APIs exposed by a Convex backend for streaming export.
#[async_trait]
pub trait Source: Display + Send {
    /// See https://docs.convex.dev/http-api/#get-apijson_schemas
    async fn json_schemas(&self) -> anyhow::Result<DatabaseSchema>;

    /// See https://docs.convex.dev/http-api/#get-apilist_snapshot
    async fn list_snapshot(
        &self,
        snapshot: Option<i64>,
        cursor: Option<Cursor>,
        table_name: Option<String>,
    ) -> anyhow::Result<ListSnapshotResponse>;

    /// See https://docs.convex.dev/http-api/#get-apidocument_deltas
    async fn document_deltas(
        &self,
        cursor: Cursor,
        table_name: Option<String>,
    ) -> anyhow::Result<DocumentDeltasResponse>;

    /// Wrapper around `json_schema` returning only the table and field names.
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

/// Implementation of [`Source`] accessing a real Convex deployment over HTTP.
pub struct ConvexApi {
    pub config: Config,
}

impl ConvexApi {
    /// Performs a GET HTTP request to a given endpoint of the Convex API using
    /// the given query parameters.
    async fn get<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        parameters: HashMap<&str, Option<String>>,
    ) -> anyhow::Result<T> {
        let non_null_parameters: HashMap<&str, String> = parameters
            .into_iter()
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect();

        let mut url = self
            .config
            .deploy_url
            .join("api/")
            .unwrap()
            .join(endpoint)
            .unwrap();

        // We always append `format=convex_json`, which is used by all the endpoints.
        url.query_pairs_mut()
            .extend_pairs(non_null_parameters)
            .append_pair("format", "convex_json");

        match reqwest::Client::new()
            .get(url)
            .header(CONVEX_CLIENT_HEADER, &*CONVEX_CLIENT_HEADER_VALUE)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Convex {}", self.config.deploy_key),
            )
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => Ok(resp
                .json::<T>()
                .await
                .context("Failed to deserialize query result")?),
            Ok(resp) => anyhow::bail!(
                "Call to {endpoint} on {} returned an unsuccessful response: {resp:?}",
                self.config.deploy_url
            ),
            Err(e) => anyhow::bail!(
                "Call to {endpoint} on {} caused an error: {e:?}",
                self.config.deploy_url
            ),
        }
    }
}

#[async_trait]
impl Source for ConvexApi {
    async fn json_schemas(&self) -> anyhow::Result<DatabaseSchema> {
        self.get("json_schemas", hashmap! {}).await
    }

    async fn list_snapshot(
        &self,
        snapshot: Option<i64>,
        cursor: Option<Cursor>,
        table_name: Option<String>,
    ) -> anyhow::Result<ListSnapshotResponse> {
        self.get(
            "list_snapshot",
            hashmap! {
                "snapshot" => snapshot.map(|n| n.to_string()),
                "cursor" => cursor.map(|n| n.to_string()),
                "tableName" => table_name,
            },
        )
        .await
    }

    async fn document_deltas(
        &self,
        cursor: Cursor,
        table_name: Option<String>,
    ) -> anyhow::Result<DocumentDeltasResponse> {
        self.get(
            "document_deltas",
            hashmap! {
                "cursor" => Some(cursor.to_string()),
                "tableName" => table_name,
            },
        )
        .await
    }
}

impl Display for ConvexApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.config.deploy_url.as_ref())
    }
}

#[derive(Display, Serialize, Deserialize, Debug, PartialEq, Eq, Clone, From, Into, Copy)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Cursor(pub i64);

#[derive(Deserialize, PartialEq, Eq, Hash, Display)]
pub struct TableName(pub String);

#[cfg(test)]
impl From<&str> for TableName {
    fn from(value: &str) -> Self {
        TableName(value.to_string())
    }
}

#[derive(Display)]
pub struct FieldName(pub String);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSnapshotResponse {
    /// Documents, in (id, ts) order.
    pub values: Vec<SnapshotValue>,
    /// Timestamp snapshot. Pass this in as `snapshot` to subsequent API calls.
    pub snapshot: i64,
    /// Exclusive timestamp for passing in as `cursor` to subsequent API calls.
    pub cursor: Option<String>,
    /// Continue calling the API while has_more is true.
    /// When this becomes false, the `ListSnapshotResponse.snapshot` can be used
    /// as `DocumentDeltasArgs.cursor` to get deltas after the snapshot.
    pub has_more: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentDeltasResponse {
    /// Document deltas, in timestamp order.
    pub values: Vec<SnapshotValue>,
    /// Exclusive timestamp for passing in as `cursor` to subsequent API calls.
    pub cursor: i64,
    /// Continue calling the API while has_more is true.
    pub has_more: bool,
}

/// A value returned by the list snapshot and document deltas API.
/// This corresponds to a Convex document with some special fields added.
#[derive(Deserialize, Debug, Clone)]
pub struct SnapshotValue {
    /// The name of the table this document is from.
    #[serde(rename = "_table")]
    pub table: String,

    /// In the document deltas API, this indicates whether the document was
    /// deleted. Will always be `false` in the list snapshot API.
    #[serde(rename = "_deleted", default)]
    pub deleted: bool,

    /// The fields of the document. Will be empty if `deleted == true`.
    /// This can contain some special system fields that are not part of the
    /// original document. All fields prefixed by `_` and that are not `_id` or
    /// `_creationTime` must be ignored.
    #[serde(flatten)]
    pub fields: HashMap<String, JsonValue>,
}

#[derive(Deserialize)]
pub struct DatabaseSchema(pub HashMap<TableName, Schema>);

#[cfg(test)]
mod tests {
    use core::panic;

    use serde_json::json;

    use super::*;

    #[test]
    fn can_deserialize_schema() {
        let json = json!({
            "emptyTable": false,
            "table": json!({
                "type": "object",
                "properties": json!({
                    "_creationTime": json!({ "type": "number" }),
                    "_id": json!({
                        "$description": "Id(messages)",
                        "type": "string"
                    }),
                    "author": json!({ "type": "string" }),
                    "body": json!({ "type": "string" }),
                    "_table": json!({ "type": "string" }),
                    "_ts": json!({ "type": "integer" }),
                    "_deleted": json!({ "type": "boolean" }),
                }),
                "additionalProperties": false,
                "required": vec!["_creationTime", "_id", "author", "body"],
                "$schema": "http://json-schema.org/draft-07/schema#",
            }),
        });

        let schema: DatabaseSchema = serde_json::from_value(json).unwrap();

        let Schema::Bool(_) = schema.0.get(&"emptyTable".into()).unwrap() else {
            panic!();
        };
        let Schema::Object(schema_object) = schema.0.get(&"table".into()).unwrap() else {
            panic!();
        };
        assert!(schema_object.object.is_some());
    }
}
