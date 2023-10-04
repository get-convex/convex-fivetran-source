---
name: Convex
title: Convex Source Connector for Fivetran
description:
  Documentation and setup guide for the Convex source connector for Fivetran.
---

# Convex {% availabilityBadge connector="convex" /%}

[Convex](https://convex.dev) is the fullstack TypeScript development platform.
Replace your database, server functions and glue code.

---

## Setup guide

This overview will give you a general idea of the capabilities of the Convex
source connector. For specific instructions on how to set it up, see the
[setup guide](/docs/databases/convex/setup-guide).

---

## Sync overview

Once Fivetran is connected to your Convex deployment, the connector fetches an
initial consistent snapshot of all data from your Convex database. Once the
initial sync is complete, the connector uses CDC to efficiently incrementally
sync updates at a newer consistent view of your Convex deployment. You can
configure the frequency of these updates.

---

## Configuration

You will need your Deployment URL and Deploy Key in order to configure the
Convex Source Connector for Fivetran. You can find both on your project's
[Production Deployment Settings page](https://docs.convex.dev/dashboard/deployments/deployment-settings).

---

## Schema information

Fivetran tries to replicate the database and columns from your configured Convex
deployment to your destination according to Fivetran's
[standard database update strategies](/docs/databases#transformationandmappingoverview).

### Type transformations and mapping

As the connector extracts your data, it matches
[Convex data types](https://docs.convex.dev/database/types) to types that
Fivetran supports.

The following table illustrates how the connector transforms your Convex data
types into Fivetran-supported types:

| Convex Type | Fivetran Type | Fivetran Supported |
| ----------- | ------------- | ------------------ |
| Id          | STRING        | True               |
| Null        | NULL          | True               |
| Int64       | LONG          | True               |
| Float64     | DOUBLE        | True               |
| Boolean     | BOOLEAN       | True               |
| String      | STRING        | True               |
| Bytes       | BINARY        | True               |
| Array       | JSON          | True               |
| Object      | JSON          | True               |

> NOTE: The system field `_creationTime` in each document is special cased to
> convert into a UTC_DATETIME, despite being stored as a Float64 inside of
> Convex.

> NOTE: Nested types inside of Object and Array are serialized as JSON using the
> export format documented [here](https://docs.convex.dev/database/types)

### Nested data

Convex documents are represented as JSON using the conversions listed
[here](https://docs.convex.dev/database/types). If the first-level field is a
simple data type, the source connector will map it to its own type. If it's a
complex nested data type such as an array or JSON data, it maps to a JSON type
without unpacking. The connector does not automatically unpack nested JSON
objects to separate tables in the destination. Any nested JSON objects are
preserved as is in the destination so that you can use JSON processing
functions.

For example, the following Convex Document...

```json
{"street"  : "Main St."
"city"     : "New York"
"country"  : "US"
"phone"    : "(555) 123-5555"
"zip code" : 12345
"people"   : ["John", "Jane", "Adam"]
"car"      : {"make" : "Honda",
              "year" : 2014,
              "type" : "AWD"}
}
```

...is converted to the following table when the connector loads it into your
destination:

| \_id | street   | city     | country | phone          | zip code | people                   | car                                               |
| ---- | -------- | -------- | ------- | -------------- | -------- | ------------------------ | ------------------------------------------------- |
| 1    | Main St. | New York | US      | (555) 123-5555 | 12345    | ["John", "Jane", "Adam"] | {"make" : "Honda", "year" : 2014, "type" : "AWD"} |

### Fivetran-generated column

Fivetran adds the following column to every table in your destination:

- `_fivetran_synced` (UTC TIMESTAMP) indicates the time when Fivetran last
  successfully synced the row.

Fivetran adds this column to give you insight into the state of your data and
the progress of your data syncs.