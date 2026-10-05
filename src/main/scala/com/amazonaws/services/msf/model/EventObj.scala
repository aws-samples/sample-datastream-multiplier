package com.amazonaws.services.msf.model

import net.datafaker.Faker
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.util.{Try, Success, Failure}
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.{Random => JavaRandom}

case class NYCTrip(
                    tripId: Long,
                    timestamp: Long,
                    fromLocationName: String,
                    toLocationName: String,
                    distance: Float,
                    amount: Float,
                    category: String
                  )

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

  // ✅ CRITICAL FIX: Reuse Random instance, just change seed
  def reseed(newSeed: Long): Unit = {
    currentSeed = newSeed
    random.setSeed(newSeed)  // No new object creation!
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

  // ✅ NEW: Reusable instances - created once and shared
  // These are lazy vals so they're only created when first accessed
  private lazy val defaultConfigLoader = new FakerConfigLoader("faker-config.yaml")
  private lazy val defaultFaker = new SeededDataFaker(0L)

  // Updated apply method - reuses instances
  def apply(
             numberOfSessions: Long,
             index: Long,
             seed: Long,
             configPath: String
           ): NYCTrip = {

    // Reseed the shared faker instance (no new object creation)
    defaultFaker.reseed(seed + index)

    val configs = defaultConfigLoader.getAllFieldConfigs
    val timestamp = System.currentTimeMillis()

    NYCTrip(
      tripId = index,
      timestamp = timestamp,
      fromLocationName = defaultFaker.generateValue(
        configs("from_location_name").expression,
        configs("from_location_name").expressionType
      ).asInstanceOf[String],
      toLocationName = defaultFaker.generateValue(
        configs("to_location_name").expression,
        configs("to_location_name").expressionType
      ).asInstanceOf[String],
      distance = defaultFaker.generateValue(
        configs("distance").expression,
        configs("distance").expressionType
      ).asInstanceOf[Float],
      amount = defaultFaker.generateValue(
        configs("amount").expression,
        configs("amount").expressionType
      ).asInstanceOf[Float],
      category = defaultFaker.generateValue(
        configs("category").expression,
        configs("category").expressionType
      ).asInstanceOf[String]
    )
  }

  // Updated randomizeFields - accepts reusable instances
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