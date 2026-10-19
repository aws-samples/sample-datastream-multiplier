package com.amazonaws.services.msf.model

import net.datafaker.Faker
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.util.{Try, Success, Failure}
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.{Random => JavaRandom}

/**
 * A Flink POJO. A Scala case class would not qualify: Flink's type extractor
 * rejects it as a POJO (immutable `val` fields have no setters, and the only
 * constructor requires all arguments). Without a valid POJO or an explicit
 * TypeInformation, Flink falls back to its generic Kryo-based serializer,
 * which is slower and gives up POJO-specific optimizations (field-level
 * serialization, more efficient equality/hashing for keyed operations).
 *
 * To qualify as a Flink POJO, a class must have:
 *   - a public no-arg constructor
 *   - all non-static, non-transient fields either public, or private with
 *     public getter/setter pairs following the getXxx/setXxx or isXxx/setXxx
 *     convention
 *   - no non-serializable fields
 * See org.apache.flink.api.java.typeutils.TypeExtractor / PojoTypeInfo.
 *
 * `var` fields with explicit get/set methods satisfy this directly, and are
 * also bean-style enough for Jackson (with jackson-module-scala) to bind to
 * without any annotations.
 */
class NYCTrip() extends Serializable {
  private var _tripId: Long = 0L
  private var _timestamp: Long = 0L
  private var _fromLocationName: String = ""
  private var _toLocationName: String = ""
  private var _distance: Float = 0f
  private var _amount: Float = 0f
  private var _category: String = ""

  def this(
      tripId: Long,
      timestamp: Long,
      fromLocationName: String,
      toLocationName: String,
      distance: Float,
      amount: Float,
      category: String
  ) = {
    this()
    this._tripId = tripId
    this._timestamp = timestamp
    this._fromLocationName = fromLocationName
    this._toLocationName = toLocationName
    this._distance = distance
    this._amount = amount
    this._category = category
  }

  def getTripId: Long = _tripId
  def setTripId(value: Long): Unit = _tripId = value

  def getTimestamp: Long = _timestamp
  def setTimestamp(value: Long): Unit = _timestamp = value

  def getFromLocationName: String = _fromLocationName
  def setFromLocationName(value: String): Unit = _fromLocationName = value

  def getToLocationName: String = _toLocationName
  def setToLocationName(value: String): Unit = _toLocationName = value

  def getDistance: Float = _distance
  def setDistance(value: Float): Unit = _distance = value

  def getAmount: Float = _amount
  def setAmount(value: Float): Unit = _amount = value

  def getCategory: String = _category
  def setCategory(value: String): Unit = _category = value

  // Scala-idiomatic read accessors so existing call sites that read fields as
  // `trip.tripId`, `trip.distance`, etc. keep working unchanged.
  def tripId: Long = _tripId
  def timestamp: Long = _timestamp
  def fromLocationName: String = _fromLocationName
  def toLocationName: String = _toLocationName
  def distance: Float = _distance
  def amount: Float = _amount
  def category: String = _category

  /** Case-class-style copy, since a plain class doesn't get one for free. */
  def copy(
      tripId: Long = this._tripId,
      timestamp: Long = this._timestamp,
      fromLocationName: String = this._fromLocationName,
      toLocationName: String = this._toLocationName,
      distance: Float = this._distance,
      amount: Float = this._amount,
      category: String = this._category
  ): NYCTrip =
    new NYCTrip(tripId, timestamp, fromLocationName, toLocationName, distance, amount, category)

  override def equals(obj: Any): Boolean = obj match {
    case other: NYCTrip =>
      tripId == other.tripId &&
        timestamp == other.timestamp &&
        fromLocationName == other.fromLocationName &&
        toLocationName == other.toLocationName &&
        distance == other.distance &&
        amount == other.amount &&
        category == other.category
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(
      java.lang.Long.valueOf(tripId),
      java.lang.Long.valueOf(timestamp),
      fromLocationName,
      toLocationName,
      java.lang.Float.valueOf(distance),
      java.lang.Float.valueOf(amount),
      category
    )

  override def toString: String =
    s"NYCTrip(tripId=$tripId, timestamp=$timestamp, fromLocationName=$fromLocationName, " +
      s"toLocationName=$toLocationName, distance=$distance, amount=$amount, category=$category)"
}

// Configuration case classes
case class FieldConfig(expression: String, expressionType: String)
case class TripGenerationConfig(trip_generation: Map[String, FieldConfig])

// Make these classes public so they can be reused
class FakerConfigLoader(configPath: String) {
  private val mapper = new ObjectMapper(new YAMLFactory())
  mapper.registerModule(DefaultScalaModule)

  private val config: TripGenerationConfig = loadConfig()

  private def loadConfig(): TripGenerationConfig = {
    Try {
      val configStream = getClass.getClassLoader.getResourceAsStream(configPath)
      if (configStream == null) {
        throw new RuntimeException(s"Configuration file not found: $configPath")
      }
      mapper.readValue(configStream, classOf[TripGenerationConfig])
    } match {
      case Success(cfg) => cfg
      case Failure(ex) => throw new RuntimeException(s"Failed to load faker config: ${ex.getMessage}", ex)
    }
  }

  def getFieldConfig(fieldName: String): Option[FieldConfig] = {
    config.trip_generation.get(fieldName)
  }

  def getAllFieldConfigs: Map[String, FieldConfig] = config.trip_generation
}

class SeededDataFaker(initialSeed: Long) {
  private var currentSeed: Long = initialSeed
  private val random: JavaRandom = new JavaRandom(initialSeed)
  private val faker: Faker = new Faker(random)

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
  private val dateFormatterNoMillis = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  // Reuses the existing Random instance and just changes its seed, avoiding
  // a new object allocation per reseed.
  def reseed(newSeed: Long): Unit = {
    currentSeed = newSeed
    random.setSeed(newSeed)
  }

  def generateValue(expression: String, expressionType: String): Any = {
    val rawValue = faker.expression(expression)

    expressionType.toLowerCase match {
      case "int" => rawValue.toInt
      case "long" => rawValue.toLong
      case "float" => rawValue.toFloat
      case "double" => rawValue.toDouble
      case "string" => rawValue
      case "timestamp" =>
        Try(rawValue.toLong).getOrElse {
          Try {
            LocalDateTime.parse(rawValue, dateFormatter)
              .toInstant(ZoneOffset.UTC)
              .toEpochMilli
          }.getOrElse {
            LocalDateTime.parse(rawValue, dateFormatterNoMillis)
              .toInstant(ZoneOffset.UTC)
              .toEpochMilli
          }
        }
      case _ => rawValue
    }
  }
}

object NYCTrip {

  // Factory method enabling case-class-style construction: NYCTrip(...) with
  // positional or named arguments.
  def apply(
      tripId: Long,
      timestamp: Long,
      fromLocationName: String,
      toLocationName: String,
      distance: Float,
      amount: Float,
      category: String
  ): NYCTrip =
    new NYCTrip(tripId, timestamp, fromLocationName, toLocationName, distance, amount, category)

  /**
   * Generates a synthetic NYCTrip using the given FakerConfigLoader and
   * SeededDataFaker.
   *
   * The caller is expected to own one FakerConfigLoader/SeededDataFaker pair
   * per Flink subtask (created once, in open()), and never share that pair
   * across subtasks or threads. SeededDataFaker.reseed() mutates PRNG state
   * with no internal synchronization, so a shared instance accessed from
   * multiple subtask threads concurrently (e.g. because DataGeneratorSource
   * runs at parallelism > 1) could interleave reseed()/generateValue() calls
   * and corrupt output. See DataMultiplierJob.NYCTripGeneratorFunction and
   * StreamMultiplier for the per-subtask ownership pattern.
   */
  def generate(
      index: Long,
      seed: Long,
      configLoader: FakerConfigLoader,
      faker: SeededDataFaker
  ): NYCTrip = {
    faker.reseed(seed + index)

    val configs = configLoader.getAllFieldConfigs
    val timestamp = System.currentTimeMillis()

    NYCTrip(
      tripId = index,
      timestamp = timestamp,
      fromLocationName = faker.generateValue(
        configs("from_location_name").expression,
        configs("from_location_name").expressionType
      ).asInstanceOf[String],
      toLocationName = faker.generateValue(
        configs("to_location_name").expression,
        configs("to_location_name").expressionType
      ).asInstanceOf[String],
      distance = faker.generateValue(
        configs("distance").expression,
        configs("distance").expressionType
      ).asInstanceOf[Float],
      amount = faker.generateValue(
        configs("amount").expression,
        configs("amount").expressionType
      ).asInstanceOf[Float],
      category = faker.generateValue(
        configs("category").expression,
        configs("category").expressionType
      ).asInstanceOf[String]
    )
  }

  def randomizeFields(
                       baseTrip: NYCTrip,
                       eventHash: Long,
                       fieldsToRandomize: Set[String],
                       configLoader: FakerConfigLoader,
                       faker: SeededDataFaker
                     ): NYCTrip = {

    if (fieldsToRandomize.isEmpty) return baseTrip

    // Reseed the faker with the event hash for deterministic randomness
    faker.reseed(eventHash)

    val newFromLocationName = if (fieldsToRandomize.contains("fromLocationName")) {
      configLoader.getFieldConfig("from_location_name").map { config =>
        faker.generateValue(config.expression, config.expressionType).asInstanceOf[String]
      }.getOrElse(baseTrip.fromLocationName)
    } else baseTrip.fromLocationName

    val newToLocationName = if (fieldsToRandomize.contains("toLocationName")) {
      configLoader.getFieldConfig("to_location_name").map { config =>
        faker.generateValue(config.expression, config.expressionType).asInstanceOf[String]
      }.getOrElse(baseTrip.toLocationName)
    } else baseTrip.toLocationName

    val newDistance = if (fieldsToRandomize.contains("distance")) {
      configLoader.getFieldConfig("distance").map { config =>
        faker.generateValue(config.expression, config.expressionType).asInstanceOf[Float]
      }.getOrElse(baseTrip.distance)
    } else baseTrip.distance

    val newAmount = if (fieldsToRandomize.contains("amount")) {
      configLoader.getFieldConfig("amount").map { config =>
        faker.generateValue(config.expression, config.expressionType).asInstanceOf[Float]
      }.getOrElse(baseTrip.amount)
    } else baseTrip.amount

    val newCategory = if (fieldsToRandomize.contains("category")) {
      configLoader.getFieldConfig("category").map { config =>
        faker.generateValue(config.expression, config.expressionType).asInstanceOf[String]
      }.getOrElse(baseTrip.category)
    } else baseTrip.category

    baseTrip.copy(
      fromLocationName = newFromLocationName,
      toLocationName = newToLocationName,
      distance = newDistance,
      amount = newAmount,
      category = newCategory
    )
  }
}