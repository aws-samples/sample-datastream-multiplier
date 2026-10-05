
package com.amazonaws.services.msf.process

import com.amazonaws.services.msf.model.{NYCTrip, FakerConfigLoader, SeededDataFaker}
import org.apache.flink.api.common.functions.{OpenContext, RichFlatMapFunction}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import scala.util.Try

trait CustomRichFlatMapMultiplier[IN, OUT] extends RichFlatMapFunction[IN, OUT] {

  val keyFields: Set[String]
  val multiplier: Int
  val randomSeedInitializers: List[Long]

  protected def emitMultipliedEvent(value: IN, out: Collector[OUT], counter: Int): Unit

  protected def emitOriginalEvent(value: IN, out: Collector[OUT]): Unit

  protected def isValidEvent(value: IN): Boolean

  override def flatMap(value: IN, out: Collector[OUT]): Unit = {
    if (isValidEvent(value)) {
      emitOriginalEvent(value, out)
      for (i <- 1 until multiplier) {
        emitMultipliedEvent(value, out, i)
      }
    }
  }
}

class StreamMultiplier(fakerConfig: String, properties: Properties)
  extends CustomRichFlatMapMultiplier[NYCTrip, NYCTrip] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // Secondary constructor for backward compatibility
  def this() = this("", new Properties())

  // Mark as transient to avoid serialization issues
  @transient private var configLoader: FakerConfigLoader = _
  @transient private var faker: SeededDataFaker = _

  override val keyFields: Set[String] = properties.get("key.fields").toString.split(",").map(_.trim).toSet
  override val multiplier: Int = properties.get("multiple.factor").toString.toInt
  override val randomSeedInitializers: List[Long] = properties.get("random.seeds").toString.split(",").map(_.trim.toLong).toList
  private val updatableFields: Set[String] = properties.get("updatable.fields").toString.split(",").map(_.trim).toSet

  // Memory monitoring
  private var processedCount: Long = 0
  private val MEMORY_LOG_INTERVAL = 10000

  override def open(ctx: OpenContext): Unit = {
    super.open(ctx)
    val subtaskIndex = getRuntimeContext.getTaskInfo.getIndexOfThisSubtask
    logger.info(s"Opening StreamMultiplier for subtask $subtaskIndex")

    // Initialize ONCE per task - these will be reused for all events
    configLoader = new FakerConfigLoader(fakerConfig)
    faker = new SeededDataFaker(0L) // Initial seed, will be reseeded per event

    val runtime = Runtime.getRuntime
    val initialHeap = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
    logger.info(s"Initialized FakerConfigLoader and SeededDataFaker for subtask $subtaskIndex. Initial heap: ${initialHeap}MB")
  }

  override def close(): Unit = {
    val runtime = Runtime.getRuntime
    val finalHeap = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
    logger.info(s"Closing StreamMultiplier. Processed $processedCount events. Final heap: ${finalHeap}MB")

    // Clean up resources
    configLoader = null
    faker = null
    super.close()
  }

  override protected def isValidEvent(event: NYCTrip): Boolean = {
    Option(event).isDefined
  }

  override protected def emitOriginalEvent(event: NYCTrip, out: Collector[NYCTrip]): Unit = {
    out.collect(event)
  }

  override protected def emitMultipliedEvent(event: NYCTrip, out: Collector[NYCTrip], counter: Int): Unit = {
    try {
      val eventHash = computeEventHash(event, keyFields, randomSeedInitializers, counter)

      // Pass the reusable instances - NO NEW OBJECTS CREATED
      val multipliedEvent = NYCTrip.randomizeFields(
        event,
        eventHash,
        updatableFields,
        configLoader,  // Reuse singleton instance
        faker          // Reuse singleton instance (will be reseeded)
      )
      out.collect(multipliedEvent)

      // Memory monitoring
      processedCount += 1
      if (processedCount % MEMORY_LOG_INTERVAL == 0) {
        val runtime = Runtime.getRuntime
        val usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
        val maxMemory = runtime.maxMemory() / 1024 / 1024
        logger.info(s"Processed $processedCount events. Memory: ${usedMemory}MB / ${maxMemory}MB")
      }

    } catch {
      case ex: Exception => throw ex
    }
  }

  private def computeEventHash(trip: NYCTrip,
                               keyFields: Set[String],
                               randomSeedInitializers: List[Long],
                               counter: Int): Long = {
    val baseHash = randomSeedInitializers.lift(counter - 1).getOrElse(0L)

    var fieldHashSum = 0L

    if (keyFields.contains("tripId")) fieldHashSum += trip.tripId.hashCode.toLong
    if (keyFields.contains("timestamp")) fieldHashSum += trip.timestamp.hashCode.toLong
    if (keyFields.contains("fromLocationName")) fieldHashSum += trip.fromLocationName.hashCode.toLong
    if (keyFields.contains("toLocationName")) fieldHashSum += trip.toLocationName.hashCode.toLong
    if (keyFields.contains("distance")) fieldHashSum += trip.distance.hashCode.toLong
    if (keyFields.contains("amount")) fieldHashSum += trip.amount.hashCode.toLong
    if (keyFields.contains("category")) fieldHashSum += trip.category.hashCode.toLong

    baseHash + fieldHashSum
  }
}