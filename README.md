# cdk-aws-serverless-otlp-forwarder-kinesis

CDK app showcasing a serverless approach to send OpenTelemetry traces to any OTel-compatible vendor using Kinesis Data Streams and Lambda.

### Related Apps

- [cdk-aws-serverless-otlp-forwarder-cwl](https://github.com/garysassano/cdk-aws-serverless-otlp-forwarder-cwl) - Uses CloudWatch Logs as OTLP transport layer instead of Kinesis Data Streams.

- [cdk-aws-serverless-otlp-ch-forwarder-kinesis](https://github.com/garysassano/cdk-aws-serverless-otlp-ch-forwarder-kinesis) - Sends traces to ClickHouse instead of an OTel-compatible vendor.

- [cdk-aws-serverless-otlp-ch-forwarder-cwl](https://github.com/garysassano/cdk-aws-serverless-otlp-ch-forwarder-cwl) - Sends traces to ClickHouse instead of an OTel-compatible vendor; uses CloudWatch Logs as OTLP transport layer instead of Kinesis Data Streams.

## Prerequisites

- **_AWS:_**
  - Must have authenticated with [Default Credentials](https://docs.aws.amazon.com/cdk/v2/guide/cli.html#cli_auth) in your local environment.
  - Must have completed the [CDK bootstrapping](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping.html) for the target AWS environment.
- **_OTel Vendor:_**
  - Must have set the `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_EXPORTER_OTLP_HEADERS` variables in your local environment.
- **_Node.js + npm:_**
  - Must be [installed](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm) in your system.
- **_Docker:_**
  - Must be [installed](https://docs.docker.com/get-docker/) in your system and running at deployment.

## Installation

```sh
npx projen install
```

## Deployment

```sh
npx projen deploy
```

## Cleanup

```sh
npx projen destroy
```

## Application Diagram

![Application Diagram](./src/assets/app-diagram.svg)

## Observability Diagram

![Observability Diagram](./src/assets/o11y-diagram.svg)
