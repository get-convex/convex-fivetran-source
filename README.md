# Fivetran Source Connector

This crate contains a source connector allowing developers using Convex to
replicate the data they store in Convex to other databases.

The connector consists of a gRPC server hosted on the Fivetran infrastructure.
This server retrieves the data it needs using the HTTP API described
[in the Convex docs](https://docs.convex.dev/http-api/).
