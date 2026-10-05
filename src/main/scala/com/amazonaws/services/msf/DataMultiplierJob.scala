package com.amazonaws.services.msf

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime
import com.amazonaws.services.msf.model.NYCTrip
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
  private val MIN_MULTIPLIER: Int = 1
  private val MAX_MULTIPLIER: Int = 100
  private val DEFAULT_MULTIPLIER: Int = 1

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

  private def createSource(env: StreamExecutionEnvironment): KinesisStreamsSource[NYCTrip] = {

    val applicationProperties = getApplicationProperties(env)
    val inputProperties = applicationProperties.get("InputStream0")

    val sourceConfig = new Configuration()
    sourceConfig.set(KinesisSourceConfigOptions.STREAM_INITIAL_POSITION, KinesisSourceConfigOptions.InitialPosition.TRIM_HORIZON) // Or LATEST, AT_TIMESTAMP, etc.

    KinesisStreamsSource.builder[NYCTrip].setSourceConfig(sourceConfig).setStreamArn(inputProperties.getProperty(streamArnKey, defaultInputStreamArn)).setDeserializationSchema(new JsonDeserializationSchema()).build()

  }

  private def createDataGenSource(env: StreamExecutionEnvironment, rateLimit: Long): DataGeneratorSource[NYCTrip] = {

    val numberOfSessions = Long.MaxValue
    //val numberOfSessions = 2L
    val rateLimiter = RateLimiterStrategy.perSecond(rateLimit)

    val generatorFunction: GeneratorFunction[java.lang.Long, NYCTrip] = (index: java.lang.Long) => NYCTrip(numberOfSessions, index.longValue(), System.currentTimeMillis(), FAKER_CONFIG_FILE)


    new DataGeneratorSource[NYCTrip](
      generatorFunction,
      numberOfSessions,  // number of events to generate
      rateLimiter,
      TypeInformation.of(classOf[NYCTrip])
    )
  }

  private def createSink(env: StreamExecutionEnvironment): KinesisStreamsSink[NYCTrip] = {
    val applicationProperties = getApplicationProperties(env)
    val outputProperties = applicationProperties.get("OutputStream0")

    KinesisStreamsSink.builder[NYCTrip]
      .setKinesisClientProperties(outputProperties)
      .setSerializationSchema(new JsonSerializationSchema())
      .setStreamArn(outputProperties.getProperty(streamArnKey, defaultOutputStreamArn))
      .setPartitionKeyGenerator((element: NYCTrip) => String.valueOf(element.hashCode))
      .build
  }


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // CRITICAL: Enable object reuse to reduce GC pressure
    env.getConfig.enableObjectReuse()

    val applicationProperties = getApplicationProperties(env)
    logger.info(s"Properties loaded: ${applicationProperties}")

    val multipliedStreamProperties = applicationProperties.get("multipliedStream")
    val multiplierStr = multipliedStreamProperties.getOrDefault("multiple.factor", "1").toString

    // Get rate limit from runtime properties (default: 100000 records/sec)
    val rateLimit = multipliedStreamProperties
      .getProperty("rate.limit.per.second", "100000")
      .toLong


    /*  check if the multiplier is a valid value and has an upper bound */
    try {
      val multiplier = multiplierStr.toInt

      if (multiplier <= 0 || multiplier > MAX_MULTIPLIER) {
        throw new IllegalArgumentException(
          s"Multiplier must be between $MIN_MULTIPLIER and $MAX_MULTIPLIER, got: $multiplier"
        )
      }
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(
          s"Invalid multiplier value '$multiplierStr'. Must be a valid integer."
        )
    }


    // Source mode is selected by the source.type runtime property:
    //   kinesis (default) - amplification mode; replays a pre-recorded data stream
    //   datagen           - pure synthetic mode; generates records in-application,
    //                       bounded by rate.limit.per.second, with no input stream
    val sourceType = multipliedStreamProperties
      .getProperty("source.type", "kinesis")
      .trim
      .toLowerCase

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