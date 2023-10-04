---
name: Convex Setup Guide
title: Convex Source Connector Setup Guide
description:
  Read step-by-step instructions on how to connect your Convex deployment with
  your destination using Fivetran connectors.
menuPosition: 0
---

​

# Convex Setup Guide {% availabilityBadge connector="cosmos" /%}

​ Follow our setup guide to connect Convex to Fivetran. ​

---

​

## Prerequisites

​ To connect your Convex deployment to Fivetran, you will need:

- A Convex deployment. See [the Convex docs](https://docs.convex.dev/) to get
  started.
- Your Convex deployment's URL (e.g., `https://jaded-raven-991.convex.cloud`)
- Your Convex deployment's deploy key. Found on the
  [Production Deployment Settings](https://docs.convex.dev/dashboard/deployments/deployment-settings)
  page. ​

---

​

## Setup instructions

​

> IMPORTANT: You must have a
> [Convex Professional plan](https://www.convex.dev/plans) to use the Fivetran
> connector. ​

### <span class="step-item">Locate your Deployment Credentials</span>

1. Navigate to your deployment on the
   [Convex Dashboard](https://dashboard.convex.dev/) ​
2. Navigate to the
   [Production Deployment Settings](https://docs.convex.dev/dashboard/deployments/deployment-settings).
3. Locate your Deployment URL and Deploy Key. ​

### <span class="step-item">Finish Fivetran configuration</span>

1. In your
   [connector setup form](/docs/getting-started/fivetran-dashboard/connectors#addanewconnector),
   enter a destination schema prefix. This prefix applies to each replicated
   schema and cannot be changed once your connector is created. ​
2. Select Convex as your source connector.
3. Enter your deployment credentials.
4. Click **Save & Test**. Fivetran tests and validates our connection to your
   Convex deployment. Upon successful completion of the setup tests, you can
   sync your data using Fivetran. ​

### Setup tests

Fivetran performs the following tests to ensure that we can connect to your
Convex deployment.

- Validating that your deployment credentials.
- Ensuring you are on a
  [Convex Professional account](https://www.convex.dev/plans).

---

## Related articles

​
[<i aria-hidden="true" class="material-icons">description</i> Connector Overview](/docs/databases/convex)
​ <b> </b> ​
[<i aria-hidden="true" class="material-icons">home</i> Documentation Home](/docs/getting-started)
