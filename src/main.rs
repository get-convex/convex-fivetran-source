#![feature(coroutines)]
#![feature(iterator_try_collect)]
#![feature(lazy_cell)]

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

use std::net::{
    IpAddr,
    Ipv4Addr,
    SocketAddr,
};

use clap::Parser;
use config::AllowAllHosts;
use connector::ConvexConnector;
use fivetran_sdk::connector_server::ConnectorServer;
use serde::Serialize;
use tonic::{
    codec::CompressionEncoding,
    transport::Server,
};

/// The command-line arguments received by the connector.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The port the connector receives gRPC requests from
    #[arg(long, default_value_t = 50051)]
    port: u16,

    /// Whether the connector is allowed to use any host as deployment URL,
    /// instead of only Convex cloud deployments.
    #[arg(long)]
    allow_all_hosts: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), args.port);

    let connector = ConvexConnector {
        allow_all_hosts: AllowAllHosts(args.allow_all_hosts),
    };

    log(&format!("Starting the connector on {}", addr));
    Server::builder()
        .add_service(
            ConnectorServer::new(connector)
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip),
        )
        .serve(addr)
        .await?;

    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct LogLine<'a> {
    level: &'a str,
    message: &'a str,
    message_origin: &'a str,
}
pub fn log(message: &str) {
    let result = serde_json::to_string(&LogLine {
        level: "INFO",
        message,
        message_origin: "sdk_connector",
    });
    match result {
        Ok(msg) => println!("{msg}"),
        Err(e) => println!("Unable to serialize to json: {message}: {e}"),
    }
}
