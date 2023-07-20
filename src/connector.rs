use futures::{
    stream::BoxStream,
    StreamExt,
    TryStreamExt,
};
use tokio_stream::Stream;
use tonic::{
    Request,
    Response,
    Status,
};

use crate::{
    config::Config,
    convex_api::ConvexApi,
    fivetran_sdk::{
        connector_server::Connector,
        schema_response,
        test_response,
        Column,
        ConfigurationFormRequest,
        ConfigurationFormResponse,
        ConnectorTest,
        DataType,
        SchemaRequest,
        SchemaResponse,
        Table,
        TableList,
        TestRequest,
        TestResponse,
        UpdateRequest,
        UpdateResponse,
        UpdateResponse as FivetranUpdateResponse,
    },
    sync::{
        sync,
        Source,
        State,
    },
};

#[derive(Debug, Default)]
pub struct ConvexConnector {}

type ConnectorResult<T> = Result<Response<T>, Status>;

impl ConvexConnector {
    async fn _schema(&self, request: Request<SchemaRequest>) -> anyhow::Result<SchemaResponse> {
        let config = Config::from_parameters(request.into_inner().configuration)?;
        let source = ConvexApi { config };

        let columns = source.get_columns().await?;

        let tables = TableList {
            tables: columns
                .into_iter()
                .map(|(table_name, column_names)| Table {
                    name: table_name.to_string(),
                    columns: column_names
                        .into_iter()
                        .map(|column_name| {
                            let column_name: String = column_name.to_string();
                            Column {
                                name: column_name.clone(),
                                r#type: match column_name.as_str() {
                                    "_id" => DataType::String,
                                    "_creationTime" => DataType::UtcDatetime,
                                    _ => DataType::Unspecified,
                                } as i32,
                                primary_key: column_name == "_id",
                                decimal: None,
                            }
                        })
                        .collect(),
                })
                .collect(),
        };

        Ok(SchemaResponse {
            response: Some(schema_response::Response::WithoutSchema(tables)),
        })
    }
}

pub trait SizedStream: Stream<Item = <Self as SizedStream>::Item> + Sized {
    type Item;
}

#[tonic::async_trait]
impl Connector for ConvexConnector {
    type UpdateStream = BoxStream<'static, Result<UpdateResponse, Status>>;

    async fn configuration_form(
        &self,
        _: Request<ConfigurationFormRequest>,
    ) -> ConnectorResult<ConfigurationFormResponse> {
        Ok(Response::new(ConfigurationFormResponse {
            schema_selection_supported: false,
            table_selection_supported: false,
            fields: Config::fivetran_fields(),
            tests: vec![ConnectorTest {
                name: "select".to_string(),
                label: "Tests selection".to_string(),
            }],
        }))
    }

    async fn test(&self, request: Request<TestRequest>) -> ConnectorResult<TestResponse> {
        let config = match Config::from_parameters(request.into_inner().configuration) {
            Ok(config) => config,
            Err(error) => {
                return Ok(Response::new(TestResponse {
                    response: Some(test_response::Response::Failure(error.to_string())),
                }));
            },
        };
        let source = ConvexApi { config };

        // Perform an API request to verify if the credentials work
        match source.json_schemas().await {
            Ok(_) => Ok(Response::new(TestResponse {
                response: Some(test_response::Response::Success(true)),
            })),
            Err(e) => Ok(Response::new(TestResponse {
                response: Some(test_response::Response::Failure(e.to_string())),
            })),
        }
    }

    async fn schema(&self, request: Request<SchemaRequest>) -> ConnectorResult<SchemaResponse> {
        self._schema(request)
            .await
            .map(Response::new)
            .map_err(|error| Status::internal(error.to_string()))
    }

    async fn update(&self, request: Request<UpdateRequest>) -> ConnectorResult<Self::UpdateStream> {
        let inner = request.into_inner();
        let config = match Config::from_parameters(inner.configuration) {
            Ok(config) => config,
            Err(error) => {
                return Err(Status::internal(error.to_string()));
            },
        };
        let state: State = match serde_json::from_str(&inner.state_json.unwrap_or("{}".to_string()))
        {
            Ok(state) => state,
            Err(error) => {
                return Err(Status::internal(error.to_string()));
            },
        };

        let source = ConvexApi { config };

        let sync = sync(source, state);
        Ok(Response::new(
            sync.map_ok(FivetranUpdateResponse::from)
                .map_err(|error| Status::internal(error.to_string()))
                .boxed(),
        ))
    }
}
