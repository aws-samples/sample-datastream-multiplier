package com.amazonaws.services.msf

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.amazonaws.services.msf.model.{NYCTrip, FakerConfigLoader, SeededDataFaker}
import com.amazonaws.services.msf.process.StreamMultiplier
import com.amazonaws.services.msf.serde.{JsonDeserializationSchema, JsonSerializationSchema}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, StreamExecutionEnvironment}
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions
import org.apache.flink.connector.datagen.source.{DataGeneratorSource, GeneratorFunction}
import org.apache.flink.api.connector.source.SourceReaderContext

import java.util
import java.util.{Properties, UUID}
import org.apache.flink.streaming.api.datastream.DataStream

import java.nio.charset.StandardCharsets
import org.slf4j.{Logger, LoggerFactory}


object DataMultiplierJob {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val streamArnKey = "stream.arn"
  private val defaultInputStreamArn = "arn:aws:kinesis:us-west-2:123456789012:stream/sample_replay_input_stream"
  private val defaultOutputStreamArn = "arn:aws:kinesis:us-west-2:123456789012:stream/sample_replay_output_stream"
  private val localApplicationPropertiesResource = "/flink-application-properties-dev.json"

  private val REPLAYSRC_FLATMAP = "REPLAY_SRC_MLTPLR"
  private val FAKER_CONFIG_FILE = "faker-config.yaml"
  private val MIN_MULTIPLIER: Int = RuntimeConfig.MIN_MULTIPLIER
  private val MAX_MULTIPLIER: Int = RuntimeConfig.MAX_MULTIPLIER
  private val DEFAULT_MULTIPLIER: Int = 1

  // Allowlist for the one-line property summary logged at startup. Logging
  // the full property map would risk emitting any credential or secret an
  // operator mistakenly placed in a runtime property; this project's own
  // template only ever puts ARNs/regions/tuning knobs here, but the job
  // should not assume that of every caller.
  private val LoggableRuntimePropertyKeys: Set[String] =
    Set("aws.region", "source.type", "multiple.factor", "key.fields", "updatable.fields",
      "rate.limit.per.second")

  private def getApplicationProperties(env: StreamExecutionEnvironment): util.Map[String, Properties] = {
    env match {
      case _: LocalStreamEnvironment =>
        // Local development: read from resources
        val localPath = getClass.getResource(localApplicationPropertiesResource).getPath
        logger.info(s"Running locally - loading properties from: $localPath")
        KinesisAnalyticsRuntime.getApplicationProperties(localPath)
      case _ =>
        // MSF environment: read from runtime properties (no argument)
        logger.info("Running in MSF - loading runtime properties")
        KinesisAnalyticsRuntime.getApplicationProperties()
    }
  }

  /**
   * Logs a redacted summary of the loaded runtime properties instead of the
   * full property map. Only allowlisted, non-sensitive keys are included;
   * stream ARNs and anything else are omitted since a caller could place
   * credentials or other sensitive values in a runtime property.
   */
  private def logSafeProperties(applicationProperties: util.Map[String, Properties]): Unit = {
    // This project targets Scala 2.12 (see build.sbt), which does not have
    // scala.jdk.CollectionConverters (added in 2.13); use the 2.12 equivalent.
    import scala.collection.JavaConverters._
    val safeView = applicationProperties.asScala.map { case (groupId, props) =>
      val safeEntries = props.asScala
        .filter { case (key, _) => LoggableRuntimePropertyKeys.contains(key) }
        .toMap
      groupId -> safeEntries
    }
    logger.info(s"Runtime property groups loaded (sensitive/unlisted keys redacted): $safeView")
  }

  /**
   * java.util.Map#get returns null for a missing key. Each runtime property
   * group (InputStream0, OutputStream0, multipliedStream) is read this way,
   * and passing a null Properties straight into code that calls
   * .getProperty(...) on it would NPE with a confusing stack trace instead of
   * naming the actual missing group. Centralizes that guard for all of them.
   */
  private def requirePropertyGroup(
      applicationProperties: util.Map[String, Properties],
      groupId: String
  ): Properties = {
    val group = applicationProperties.get(groupId)
    if (group == null) {
      throw new IllegalArgumentException(s"Missing required runtime property group '$groupId'.")
    }
    group
  }

  private def createSource(env: StreamExecutionEnvironment): KinesisStreamsSource[NYCTrip] = {

    val applicationProperties = getApplicationProperties(env)
    val inputProperties = requirePropertyGroup(applicationProperties, "InputStream0")

    val sourceConfig = new Configuration()
    sourceConfig.set(KinesisSourceConfigOptions.STREAM_INITIAL_POSITION, KinesisSourceConfigOptions.InitialPosition.TRIM_HORIZON) // Or LATEST, AT_TIMESTAMP, etc.

    KinesisStreamsSource.builder[NYCTrip].setSourceConfig(sourceConfig).setStreamArn(inputProperties.getProperty(streamArnKey, defaultInputStreamArn)).setDeserializationSchema(new JsonDeserializationSchema()).build()

  }

  /**
   * Generates synthetic NYCTrip records for the datagen source path.
   *
   * GeneratorFunction supports the same open()/close() lifecycle as a
   * RichFunction (called once per subtask before mapping begins), so each
   * subtask gets its own FakerConfigLoader/SeededDataFaker here. This matters
   * because DataGeneratorSource can run at parallelism > 1 (see
   * FlinkApplicationConfiguration.ParallelismConfiguration in the CFN
   * template): sharing a single SeededDataFaker across subtask threads would
   * let concurrent reseed()/generateValue() calls interleave and corrupt
   * output, since SeededDataFaker has no internal synchronization.
   * StreamMultiplier follows the same one-faker-per-subtask pattern for the
   * multiplier path.
   */
  // package-private (not `private`) so tests can exercise it directly.
  private[msf] class NYCTripGeneratorFunction(fakerConfig: String)
    extends GeneratorFunction[java.lang.Long, NYCTrip] {

    @transient private var configLoader: FakerConfigLoader = _
    @transient private var faker: SeededDataFaker = _

    override def open(context: SourceReaderContext): Unit = {
      configLoader = new FakerConfigLoader(fakerConfig)
      faker = new SeededDataFaker(0L)
    }

    override def map(index: java.lang.Long): NYCTrip = {
      NYCTrip.generate(index.longValue(), System.currentTimeMillis(), configLoader, faker)
    }
  }

  private def createDataGenSource(env: StreamExecutionEnvironment, rateLimit: Long): DataGeneratorSource[NYCTrip] = {

    val numberOfSessions = Long.MaxValue
    //val numberOfSessions = 2L
    val rateLimiter = RateLimiterStrategy.perSecond(rateLimit)

    val generatorFunction: GeneratorFunction[java.lang.Long, NYCTrip] = new NYCTripGeneratorFunction(FAKER_CONFIG_FILE)

    new DataGeneratorSource[NYCTrip](
      generatorFunction,
      numberOfSessions,  // number of events to generate
      rateLimiter,
      TypeInformation.of(classOf[NYCTrip])
    )
  }

  private def createSink(env: StreamExecutionEnvironment): KinesisStreamsSink[NYCTrip] = {
    val applicationProperties = getApplicationProperties(env)
    val outputProperties = requirePropertyGroup(applicationProperties, "OutputStream0")

    KinesisStreamsSink.builder[NYCTrip]
      .setKinesisClientProperties(outputProperties)
      .setSerializationSchema(new JsonSerializationSchema())
      .setStreamArn(outputProperties.getProperty(streamArnKey, defaultOutputStreamArn))
      .setPartitionKeyGenerator((element: NYCTrip) => String.valueOf(element.hashCode))
      .build
  }


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Object reuse is enabled to reduce GC pressure. This is safe for this
    // operator chain because NYCTrip (a mutable POJO) is never retained past
    // a single synchronous processing step:
    //   - StreamMultiplier.emitOriginalEvent passes the source's record
    //     straight through (out.collect(event), no copy) to the sink.
    //   - KinesisStreamsSinkWriter extends AsyncSinkWriter<NYCTrip,
    //     PutRecordsRequestEntry> (flink-connector-aws-kinesis-streams
    //     6.0.1-2.0): its internal buffer type parameter is
    //     PutRecordsRequestEntry, not NYCTrip. KinesisStreamsSinkElementConverter
    //     .apply(NYCTrip, ctx) runs JsonSerializationSchema.serialize(element)
    //     synchronously, copying bytes out immediately, and only the resulting
    //     PutRecordsRequestEntry (serialized bytes, not the POJO) is ever
    //     buffered. The NYCTrip reference itself is never retained past that
    //     single synchronous call.
    //   - Operator chaining within one task is single-threaded and
    //     synchronous: the source only produces (and could reuse-mutate) the
    //     next record after flatMap -> sink.write() -> element converter ->
    //     serialize() has already returned for the current one.
    // If StreamMultiplier, the sink, or the connector version changes,
    // re-verify this reasoning before assuming it still holds.
    env.getConfig.enableObjectReuse()

    val applicationProperties = getApplicationProperties(env)
    logSafeProperties(applicationProperties)

    // java.util.Map#get returns null for a missing key (e.g. a misconfigured
    // CloudFormation template or a local dev properties file missing the
    // "multipliedStream" group entirely). Guard against that explicitly:
    // passing null straight into RuntimeConfig.validate would NPE on its first
    // properties.getProperty(...) call, surfacing a confusing stack trace
    // instead of an actionable error naming the missing group.
    val multipliedStreamProperties = applicationProperties.get("multipliedStream")
    if (multipliedStreamProperties == null) {
      throw new IllegalArgumentException(
        "Missing required runtime property group 'multipliedStream'. Expected keys: " +
          "multiple.factor, key.fields, random.seeds, updatable.fields, rate.limit.per.second, source.type."
      )
    }

    // Validates multiple.factor, key.fields, random.seeds, updatable.fields,
    // rate.limit.per.second, and source.type together, failing with one
    // consolidated, actionable error instead of one check at a time.
    val validatedConfig = RuntimeConfig.validate(multipliedStreamProperties)
    val rateLimit = validatedConfig.rateLimitPerSecond

    // Source mode is selected by the source.type runtime property:
    //   kinesis (default) - amplification mode; replays a pre-recorded data stream
    //   datagen           - pure synthetic mode; generates records in-application,
    //                       bounded by rate.limit.per.second, with no input stream
    val sourceType = validatedConfig.sourceType

    val eventStream: DataStream[NYCTrip] = sourceType match {
      case "datagen" =>
        logger.info(s"Source mode: datagen (synthetic), rate limit $rateLimit records/sec")
        env.fromSource(
          createDataGenSource(env, rateLimit),
          WatermarkStrategy.noWatermarks(),
          "DataGen Synthetic Source"
        ).returns(TypeInformation.of(classOf[NYCTrip]))

      case "kinesis" =>
        logger.info("Source mode: kinesis (amplification)")
        env.fromSource(
          createSource(env),
          WatermarkStrategy.noWatermarks(),
          "Kinesis Source"
        ).returns(TypeInformation.of(classOf[NYCTrip]))

      case other =>
        throw new IllegalArgumentException(
          s"Invalid source.type '$other'. Must be 'kinesis' or 'datagen'."
        )
    }

    val multStream = eventStream
      .flatMap(new StreamMultiplier(FAKER_CONFIG_FILE, multipliedStreamProperties)).uid(REPLAYSRC_FLATMAP).name(REPLAYSRC_FLATMAP)

    //eventStream.print()

    multStream.sinkTo(createSink(env))
    //eventStream.sinkTo(createSink(env))

    env.execute("Event Generator Job")

  }

}