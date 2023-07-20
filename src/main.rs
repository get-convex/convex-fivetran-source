#![feature(generators)]
#![feature(iterator_try_collect)]
#![feature(once_cell)]

mod config;
mod connector;
mod convert;
mod convex_api;
mod sync;

mod fivetran_sdk {
    #![allow(clippy::enum_variant_names)]
    tonic::include_proto!("fivetran_sdk");
}

#[cfg(test)]
mod tests;

use std::{
    env,
    net::SocketAddr,
};

use connector::ConvexConnector;
use fivetran_sdk::connector_server::ConnectorServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = env::var("SOCKET_ADDRESS").unwrap_or("[::]:50051".to_string());
    let addr: SocketAddr = addr.parse().expect("Invalid socket address");

    let connector = ConvexConnector::default();

    Server::builder()
        .add_service(ConnectorServer::new(connector))
        .serve(addr)
        .await?;

    Ok(())
}
