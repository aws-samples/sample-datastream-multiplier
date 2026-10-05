# Multiply pre-recorded streams with Amazon Managed Service for Apache Flink

Companion code for the AWS Big Data Blog post of the same name.

This sample amplifies a pre-recorded Amazon Kinesis data stream into production-scale test
traffic. Each incoming record is emitted unchanged, plus a configurable number of synthetic
variants that preserve business key fields while regenerating a nominated subset of fields with
[DataFaker](https://www.datafaker.net/).

> **Source data.** This sample does not mask, anonymize, or remove personally identifiable
> information (PII). Fields not listed in `updatable.fields` pass through unchanged. Use open
> data, or data that has already been sanitized, as the input stream.

* Flink version: 2.3.0
* Scala version: 2.12.20
* Java version: 17
* Flink API: DataStream API
* Language: Scala

The project can run both on Amazon Managed Service for Apache Flink, and locally for development.

The application shows how to get runtime configuration, and sets up a Kinesis Data Stream source and a sink.

### Pre-requisites

You need to have `sbt` tool installed on your machine to build a Scala project. Use [steps from official guide](https://www.scala-sbt.org/download.html) to do that.

You also need JDK 17, which is the default Java version for Flink 2.x.

### Build

- Run `sbt assembly` to build an uber jar
- Use `target/scala-2.12/DataStreamGeneratorDemo-flink_2.3.jar` in your MSF application

> Note: This app uses the Flink 2.x Kinesis connector `flink-connector-aws-kinesis-streams`
> (`6.0.1-2.0`), which provides both the `KinesisStreamsSource` and `KinesisStreamsSink`.
> The legacy `flink-connector-kinesis` artifact is not used on Flink 2.x.

### Deploying with CloudFormation

`infrastructure/cfn-template.yaml` provisions the full stack on Amazon Managed Service for
Apache Flink. It creates:

- The Managed Service for Apache Flink application (Flink 2.3 runtime by default) with its
  runtime property groups (`multipliedStream`, `InputStream0`, `OutputStream0`) pre-wired.
- The **output** Kinesis Data Stream (multiplied sink).
- The **input** Kinesis Data Stream (source) — optional, see `CreateInputStream` below.
- IAM role/policy for the app: Kinesis read on the input stream, write on the output stream,
  plus CloudWatch Logs and metrics.
- A CloudWatch log group (KMS-encrypted) and log stream.
- A sample **producer Lambda** that pushes JSON trip records into the input stream —
  created only when this stack owns the input stream.

#### Key parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ApplicationName` | `DataStreamMultiplier` | Name of the Flink application and prefix for related resources. |
| `S3BucketArn` | _(required)_ | ARN of the S3 bucket holding the uber JAR, for example `arn:aws:s3:::my-bucket`. |
| `S3Key` | `DataStreamGeneratorDemo-flink_2.3.jar` | S3 key of the uber JAR produced by `sbt assembly`. |
| `FlinkRuntimeEnvironment` | `FLINK-2_3` | Flink runtime version. |
| `CreateInputStream` | `true` | Create the input stream (and the producer Lambda). Set `false` to reuse an existing stream named by `InputStreamName`. |
| `InputStreamName` | `sample_replay_input_stream` | Input stream to create, or the existing stream to read from. |
| `OutputStreamName` | `sample_replay_output_stream` | Output (multiplied) stream to create. |
| `InputShardCount` / `OutputShardCount` | `2` / `4` | Shard counts for the streams. |
| `SourceType` | `kinesis` | `kinesis` to replay a pre-recorded stream, `datagen` to generate synthetic records in-application. |
| `StreamRetentionHours` | `168` | Retention for both streams. 24 is the default tier; 25-168 is extended retention billed per shard-hour; above 168 is long-term retention billed per GB-month. |
| `MultipleFactor` | `10` | Copies emitted per input record (1-100). |
| `KeyFields` / `RandomSeeds` / `UpdatableFields` | see template | Multiplier randomization config. |
| `ProducerNumRecords` | `10` | Records the producer Lambda emits per invocation. |
| `LogGroupKmsKeyArn` | _(empty)_ | Optional KMS key for log encryption; a key is created if empty. |

> `CreateInputStream` controls both the input stream **and** the producer Lambda. When `true`,
> the stack owns the input stream and deploys the producer to feed it. When `false`, it reuses
> the stream named by `InputStreamName` (in the same account/region) and skips the producer,
> on the assumption its owner already produces to it.

#### Deploy

First build and upload the uber JAR, then deploy the stack:

```bash
# 1. Build the uber jar
sbt assembly

# 2. Upload it to your S3 bucket
aws s3 cp target/scala-2.12/DataStreamGeneratorDemo-flink_2.3.jar s3://<your-bucket>/

# 3. Deploy the stack
aws cloudformation deploy \
  --template-file infrastructure/cfn-template.yaml \
  --stack-name data-stream-multiplier \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
      S3BucketArn=arn:aws:s3:::<your-bucket> \
      S3Key=DataStreamGeneratorDemo-flink_2.3.jar
```

Then start the application (CloudFormation deploys it in the `READY` state):

```bash
aws kinesisanalyticsv2 start-application --application-name DataStreamMultiplier
```

#### Feeding the input stream

When `CreateInputStream=true`, the stack includes a producer Lambda
(`<ApplicationName>-producer`). Its source lives at `infrastructure/lambda/producer.py` and is
inlined into the template. Invoke it to push a batch of sample records into the input stream:

```bash
aws lambda invoke \
  --function-name DataStreamMultiplier-producer \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  out.json && cat out.json
```

Each invocation emits `ProducerNumRecords` records (default 10). The producer is invoke-on-demand
by default — there is no schedule or automatic trigger. Options to run it automatically include
an EventBridge scheduled rule (for a continuous feed) or a CloudFormation custom resource (to seed
once at deploy time); neither is wired in currently.

### Runtime configuration

The application reads the runtime configuration from the Runtime Properties, when running on Amazon Managed Service for
Apache Flink.

The following runtime properties are required:

| Group ID        | Key           | Description                                   |
|-----------------|---------------|-----------------------------------------------|
| `InputStream0`  | `stream.name` | Name of the input stream                      |
| `InputStream0`  | `aws.region`  | Region of the input stream, e.g. `us-west-2`  |
| `OutputStream0` | `stream.name` | Name of the output stream                     |
| `OutputStream0` | `aws.region`  | Region of the output stream, e.g. `us-west-2` |

- A separate configuration block to set the multiplier input parameters, such as multiply factor, stream key fields etc.
  - The `multiple.factor` parameter has an upper bound, should be a valid integer between 1 and 100

### Running in IntelliJ

You can run this example directly in IntelliJ, without any local Flink cluster or local Flink installation.

Running locally
 - Create the source & target Kinesis data streams in AWS
 - Use AWS profile or set the AWS credentials as environment variables in the IDE
 - Set the below VM parameters
```
--add-opens java.base/java.util=ALL-UNNAMED
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.lang.invoke=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.io=ALL-UNNAMED
--add-opens java.base/java.net=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/java.util.concurrent=ALL-UNNAMED
--add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED
--add-opens java.base/java.util.concurrent.locks=ALL-UNNAMED
```

### Generating data

- Flink DataGen connector is integrated to generate the initial synthetic data for testing the multiplier functionality
- For initial data generation, configure the number of rows & set the input data stream as the sink stream before running the stream application

### Stream message format

Records are JSON, one object per Kinesis record, handled by
`JsonDeserializationSchema.scala` on the way in and `JsonSerializationSchema.scala` on the way out.

```json
{
  "tripId": 0,
  "timestamp": 1726327891000,
  "fromLocationName": "Manhattan - Times Square",
  "toLocationName": "JFK Airport",
  "distance": 12.5,
  "amount": 42.75,
  "category": "1"
}
```

Field types: `tripId` (long), `timestamp` (long), `fromLocationName` (string),
`toLocationName` (string), `distance` (float), `amount` (float), `category` (string).

Rules enforced by the deserializer:

- Unknown fields are ignored, so a producer can add attributes without breaking the job.
- Whole message up to 100 KB.
- Value bounds: `tripId >= 0`, `timestamp > 0`, `0 <= distance <= 10000`, `0 <= amount <= 100000`,
  and non-empty `fromLocationName`, `toLocationName`, and `category`.

The sink writes the same shape, so the amplified output stream can be fed straight back in as the
input of another run — which is how the source data for a load test is generated in the first place
(see `source.type` below).

### Source modes

`source.type` selects where records come from. Both modes feed the same multiplication operator.

| `source.type` | Behaviour |
|---|---|
| `kinesis` (default) | Amplification mode. Replays a pre-recorded data stream from `InputStream0`. |
| `datagen` | Pure synthetic mode. Generates records in the application using `faker-config.yaml`, bounded by `rate.limit.per.second`. No input stream is read. |

To produce source data for a later load test, run in `datagen` mode with `multiple.factor=1` and
point `OutputStream0` at the stream you intend to replay later. With a factor of 1 the multiplier
emits only the original record, so the output is a clean 1:1 synthetic stream at the configured
rate. Because generation is unbounded, the application can run for as long as needed.

> Do not point `InputStream0` and `OutputStream0` at the same stream in `kinesis` mode. The
> application would consume its own output and amplify without limit.

### NYC Trips Data
- For this demo, NYC trips data with the below schema is being used to model the generated stream
```
BIGINT Trip_ID PK
BIGINT timestamp
VARCHAR fromLocationName
VARCHAR toLocationName
FLOAT distance
FLOAT amount
VARCHAR category

```
- In case if a custom schema to be used instead of the NYC trips data schema, below files need to be updated appropriately
1) StreamMultiplier.scala
2) JsonDeserializationSchema.scala / JsonSerializationSchema.scala
3) EventObj.scala
4) DataMultiplierJob.scala
5) faker-config.yaml (fields and data types)

- Note:
```
  SECURITY WARNING: This is a demo deserializer without production-grade validation.

  Before using in production, implement the following:

  CODE-LEVEL SECURITY:
    - Input size limits (e.g., 100KB max)
    - Field length validation (e.g., 256 chars per field)
    - Bounds checking for numeric conversions
    - Stricter regex patterns with bounded field lengths and allowed character sets

  OPERATIONAL SECURITY:
    - Dependency Scanning: Implement automated vulnerability scanning for dependencies
      (Jackson, Log4j, Flink, etc.) using tools like Snyk, AWS Inspector, or Dependabot.
    - Establish a regular update cadence and monitor for CVE alerts.

    - Data Classification: This application multiplies ALL data without sensitivity checks.
    - Before processing production data:
        - Identify and classify sensitive fields (PII, PHI, financial data)
        - Implement data masking/redaction for sensitive fields
        - Ensure compliance with GDPR, CCPA, and other regulations
        - Add audit logging for data access and transformations
 ```

### Clean up

The resources this stack creates continue to incur charges while they exist — the Flink
application consumes KPUs whenever it is running, and both Kinesis Data Streams are billed per
shard-hour. Remove everything once you are finished testing.

Stop the application first. CloudFormation cannot delete an application that is still running:

```bash
aws kinesisanalyticsv2 stop-application \
  --application-name DataStreamMultiplier \
  --force
```

Wait until the application reports `READY`, then delete the stack:

```bash
aws cloudformation delete-stack --stack-name data-stream-multiplier

aws cloudformation wait stack-delete-complete --stack-name data-stream-multiplier
```

That removes the Flink application, both Kinesis Data Streams, the producer Lambda, the IAM role
and policy, the log group, and the KMS key created for log encryption.

Two things the stack does not remove:

- **The uber JAR in Amazon S3.** The bucket is an input to the stack, not something it creates,
  so delete the object yourself:

  ```bash
  aws s3 rm s3://<your-bucket>/DataStreamGeneratorDemo-flink_2.3.jar
  ```

- **An input stream the stack does not own.** If you deployed with `CreateInputStream=false`, the
  input stream belongs to whoever created it and is left in place.

If you passed your own key through `LogGroupKmsKeyArn`, that key is also left alone. A key the
stack created for you is scheduled for deletion when the stack is removed.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
