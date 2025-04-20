import { join } from "node:path";
import {
  Aspects,
  CfnOutput,
  DockerImage,
  Duration,
  type IAspect,
  RemovalPolicy,
  SecretValue,
  Stack,
  type StackProps,
} from "aws-cdk-lib";
import { EndpointType, LambdaIntegration, RestApi } from "aws-cdk-lib/aws-apigateway";
import { AttributeType, TableV2 } from "aws-cdk-lib/aws-dynamodb";
import { Effect, PolicyStatement } from "aws-cdk-lib/aws-iam";
import { Stream } from "aws-cdk-lib/aws-kinesis";
import {
  ApplicationLogLevel,
  Architecture,
  FunctionUrlAuthType,
  Function as LambdaFunction,
  type LayerVersion,
  LoggingFormat,
  Runtime,
  StartingPosition,
  SystemLogLevel,
} from "aws-cdk-lib/aws-lambda";
import { KinesisEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import { Schedule, ScheduleExpression } from "aws-cdk-lib/aws-scheduler";
import { LambdaInvoke } from "aws-cdk-lib/aws-scheduler-targets";
import { Secret } from "aws-cdk-lib/aws-secretsmanager";
import { RustExtension, RustFunction } from "cargo-lambda-cdk";
import type { Construct, IConstruct } from "constructs";
import { PythonFunction } from "uv-python-lambda";
import { validateEnv } from "../utils/validate-env";

// Constants
const COLLECTORS_SECRETS_KEY_PREFIX = "serverless-otlp-forwarder/keys/";

// Required environment variables
const env = validateEnv(["OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_HEADERS"]);

export class MyStack extends Stack {
  constructor(scope: Construct, id: string, props: StackProps = {}) {
    super(scope, id, props);

    //==============================================================================
    // VENDOR SECRET (SECRETS MANAGER)
    //==============================================================================

    new Secret(this, "VendorSecret", {
      secretName: `${COLLECTORS_SECRETS_KEY_PREFIX}vendor`,
      description: "Vendor API key for OTLP forwarder",
      secretStringValue: SecretValue.unsafePlainText(
        JSON.stringify({
          name: "vendor",
          endpoint: env.OTEL_EXPORTER_OTLP_ENDPOINT,
          auth: env.OTEL_EXPORTER_OTLP_HEADERS,
        }),
      ),
    });

    //==============================================================================
    // QUOTES TABLE (DDB)
    //==============================================================================

    const quotesTable = new TableV2(this, "QuotesTable", {
      tableName: "quotes-table",
      partitionKey: { name: "pk", type: AttributeType.STRING },
      timeToLiveAttribute: "expiry",
      removalPolicy: RemovalPolicy.DESTROY,
    });

    //==============================================================================
    // BACKEND API (APIGW)
    //==============================================================================

    const backendApi = new RestApi(this, "BackendApi", {
      restApiName: "backend-api",
      endpointTypes: [EndpointType.REGIONAL],
    });
    backendApi.node.tryRemoveChild("Endpoint");

    //==============================================================================
    // APP FUNCTIONS (LAMBDA)
    //==============================================================================

    // App Backend Function
    const appBackend = new RustFunction(this, "AppBackend", {
      functionName: "app-backend",
      manifestPath: join(__dirname, "../functions/app-backend", "Cargo.toml"),
      architecture: Architecture.ARM_64,
      memorySize: 1024,
      timeout: Duration.minutes(1),
      loggingFormat: LoggingFormat.JSON,
      bundling: { cargoLambdaFlags: ["--quiet"] },
      environment: {
        TABLE_NAME: quotesTable.tableName,
      },
    });
    quotesTable.grantReadWriteData(appBackend);

    // App Frontend Function
    const appFrontend = new RustFunction(this, "AppFrontend", {
      functionName: "app-frontend",
      manifestPath: join(__dirname, "../functions/app-frontend", "Cargo.toml"),
      architecture: Architecture.ARM_64,
      memorySize: 1024,
      timeout: Duration.minutes(1),
      loggingFormat: LoggingFormat.JSON,
      bundling: { cargoLambdaFlags: ["--quiet"] },
      environment: {
        LAMBDA_EXTENSION_SPAN_PROCESSOR_MODE: "async",
        TARGET_URL: backendApi.url,
      },
    });
    const appFrontendUrl = appFrontend.addFunctionUrl({
      authType: FunctionUrlAuthType.NONE,
    });

    //==============================================================================
    // BACKEND API ROUTES (APIGW)
    //==============================================================================

    // {api}/quotes
    const quotesResource = backendApi.root.resourceForPath("/quotes");
    quotesResource.addMethod("GET", new LambdaIntegration(appBackend));
    quotesResource.addMethod("POST", new LambdaIntegration(appBackend));

    // {api}/quotes/{id}
    const quoteByIdResource = backendApi.root.resourceForPath("/quotes/{id}");
    quoteByIdResource.addMethod("GET", new LambdaIntegration(appBackend));

    //==============================================================================
    // CLIENT FUNCTIONS (LAMBDA)
    //==============================================================================

    // Client Node Function
    const clientNode = new NodejsFunction(this, "ClientNode", {
      functionName: "client-node",
      entry: join(__dirname, "../functions/client-node", "index.ts"),
      runtime: Runtime.NODEJS_22_X,
      architecture: Architecture.ARM_64,
      memorySize: 1024,
      timeout: Duration.minutes(1),
      loggingFormat: LoggingFormat.JSON,
      environment: {
        LAMBDA_EXTENSION_SPAN_PROCESSOR_MODE: "async",
        TARGET_URL: `${backendApi.url}quotes`,
      },
    });
    new Schedule(this, "ClientNodeSchedule", {
      scheduleName: `client-node-schedule`,
      description: `Trigger ${clientNode.functionName} every 5 minutes`,
      schedule: ScheduleExpression.rate(Duration.minutes(5)),
      target: new LambdaInvoke(clientNode),
    });

    // Client Python Function
    const clientPython = new PythonFunction(this, "ClientPython", {
      functionName: "client-python",
      rootDir: join(__dirname, "../functions/client-python"),
      runtime: Runtime.PYTHON_3_13,
      architecture: Architecture.ARM_64,
      memorySize: 1024,
      timeout: Duration.minutes(1),
      loggingFormat: LoggingFormat.JSON,
      bundling: {
        image: DockerImage.fromBuild(join(__dirname, "../functions/client-python")),
        assetExcludes: ["Dockerfile", ".venv"],
      },
      environment: {
        LAMBDA_EXTENSION_SPAN_PROCESSOR_MODE: "async",
        TARGET_URL: `${backendApi.url}quotes`,
      },
    });
    new Schedule(this, "ClientPythonSchedule", {
      scheduleName: `client-python-schedule`,
      description: `Trigger ${clientPython.functionName} every 5 minutes`,
      schedule: ScheduleExpression.rate(Duration.minutes(5)),
      target: new LambdaInvoke(clientPython),
    });

    // Client Rust Function
    const clientRust = new RustFunction(this, "ClientRust", {
      functionName: "client-rust",
      manifestPath: join(__dirname, "../functions/client-rust", "Cargo.toml"),
      architecture: Architecture.ARM_64,
      memorySize: 1024,
      timeout: Duration.minutes(1),
      loggingFormat: LoggingFormat.JSON,
      bundling: { cargoLambdaFlags: ["--quiet"] },
      environment: {
        LAMBDA_EXTENSION_SPAN_PROCESSOR_MODE: "async",
      },
    });
    const clientRustUrl = clientRust.addFunctionUrl({
      authType: FunctionUrlAuthType.NONE,
    });

    //==============================================================================
    // OTLP FORWARDER (LAMBDA)
    //==============================================================================

    const otlpForwarder = new RustFunction(this, "OtlpForwarder", {
      functionName: "otlp-forwarder",
      manifestPath: join(__dirname, "../functions/otlp-forwarder-kinesis", "Cargo.toml"),
      architecture: Architecture.ARM_64,
      memorySize: 1024,
      timeout: Duration.minutes(15),
      loggingFormat: LoggingFormat.JSON,
      systemLogLevelV2: SystemLogLevel.WARN,
      applicationLogLevelV2: ApplicationLogLevel.INFO,
      bundling: { cargoLambdaFlags: ["--quiet"] },
      environment: {
        // OTLP Forwarder
        COLLECTORS_CACHE_TTL_SECONDS: "300",
        COLLECTORS_SECRETS_KEY_PREFIX,
        // Lambda OTel Lite
        LAMBDA_EXTENSION_SPAN_PROCESSOR_MODE: "async",
        LAMBDA_TRACING_ENABLE_FMT_LAYER: "true",
        // OTel SDK
        OTEL_EXPORTER_OTLP_ENDPOINT: env.OTEL_EXPORTER_OTLP_ENDPOINT,
        OTEL_EXPORTER_OTLP_HEADERS: env.OTEL_EXPORTER_OTLP_HEADERS,
        OTEL_EXPORTER_OTLP_PROTOCOL: "http/protobuf",
      },
    });
    otlpForwarder.addToRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: [
          "secretsmanager:GetSecretValue",
          "secretsmanager:BatchGetSecretValue",
          "secretsmanager:ListSecrets",
        ],
        resources: ["*"],
      }),
    );

    //==============================================================================
    // OTLP TRANSPORT (KINESIS)
    //==============================================================================

    const otlpStream = new Stream(this, "otlpStream", {
      streamName: `otlp-stream`,
      shardCount: 1,
      retentionPeriod: Duration.days(1),
      removalPolicy: RemovalPolicy.DESTROY,
    });

    otlpForwarder.addEventSource(
      new KinesisEventSource(otlpStream, {
        startingPosition: StartingPosition.LATEST,
        batchSize: 100,
        maxBatchingWindow: Duration.seconds(5),
      }),
    );

    //==============================================================================
    // OTLP STDOUT KINESIS EXTENSION (LAMBDA)
    //==============================================================================

    const otlpStdoutKinesisExtension = new RustExtension(this, "OtlpStdoutKinesisExtension", {
      layerVersionName: "otlp-stdout-kinesis-extension",
      manifestPath: join(__dirname, "../layers/otlp-stdout-kinesis-extension", "Cargo.toml"),
      architecture: Architecture.ARM_64,
      bundling: { cargoLambdaFlags: ["--quiet"] },
    });

    //==============================================================================
    // APPLY ASPECTS
    //==============================================================================

    Aspects.of(this).add(new ApplyKinesisExtensionAspect(otlpStream, otlpStdoutKinesisExtension));

    //==============================================================================
    // OUTPUTS
    //==============================================================================

    new CfnOutput(this, "QuotesApiUrl", {
      value: `${backendApi.url}quotes`,
    });

    new CfnOutput(this, "AppFrontendUrl", {
      value: appFrontendUrl.url,
    });

    new CfnOutput(this, "ClientRustLambdaUrl", {
      value: clientRustUrl.url,
    });
  }
}

//==============================================================================
// CDK ASPECTS
//==============================================================================

class ApplyKinesisExtensionAspect implements IAspect {
  constructor(
    private readonly kinesisStream: Stream,
    private readonly extensionLayer: LayerVersion,
  ) {}

  public visit(node: IConstruct): void {
    if (node instanceof LambdaFunction) {
      // Add the extension layer
      node.addLayers(this.extensionLayer);

      // Grant write permissions to the Kinesis stream
      if (node.role) {
        this.kinesisStream.grantWrite(node.role);
      }

      // Add environment variables
      node.addEnvironment("OTEL_LITE_EXTENSION_STREAM_NAME", this.kinesisStream.streamName);
      node.addEnvironment("OTEL_LITE_EXTENSION_ENABLE_PLATFORM_TELEMETRY", "true");
      node.addEnvironment("OTLP_STDOUT_SPAN_EXPORTER_OUTPUT_TYPE", "pipe");
    }
  }
}
