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
    net::SocketAddr,
    str::FromStr,
};

use clap::Parser;
use config::AllowAllHosts;
use connector::ConvexConnector;
use fivetran_sdk::connector_server::ConnectorServer;
use tonic::transport::Server;

/// The command-line arguments received by the connector.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The address of the socket the connector receives gRPC requests from
    #[arg(long, default_value_t = SocketAddr::from_str("[::]:50051").unwrap())]
    socket_address: SocketAddr,

    /// Whether the connector is allowed to use any host as deployment URL,
    /// instead of only Convex cloud deployments.
    #[arg(long)]
    allow_all_hosts: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr: SocketAddr = args.socket_address;

    let connector = ConvexConnector {
        allow_all_hosts: AllowAllHosts(args.allow_all_hosts),
    };

    println!("Starting the connector on {}", addr);
    Server::builder()
        .add_service(ConnectorServer::new(connector))
        .serve(addr)
        .await?;

    Ok(())
}
