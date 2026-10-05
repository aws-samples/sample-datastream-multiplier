package com.amazonaws.services.msf.serde

import com.amazonaws.services.msf.model.NYCTrip
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets

/**
 * Reads NYCTrip records from JSON.
 *
 * Expected record shape, one JSON object per Kinesis record:
 * {
 *   "tripId": 1,
 *   "timestamp": 1726327891000,
 *   "fromLocationName": "Manhattan - Times Square",
 *   "toLocationName": "JFK Airport",
 *   "distance": 12.5,
 *   "amount": 42.75,
 *   "category": "1"
 * }
 *
 * Unknown fields are ignored, so a producer can add attributes without breaking
 * this deserializer.
 */
class JsonDeserializationSchema extends DeserializationSchema[NYCTrip] {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // Guard against oversized records before handing anything to the parser.
  private val MaxInputSize = 102400 // 100 KB

  // ObjectMapper is not serializable, so it is rebuilt on each task manager.
  @transient private lazy val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    m
  }

  override def deserialize(message: Array[Byte]): NYCTrip = {
    if (message == null || message.isEmpty) {
      logger.warn("Received null or empty message")
      return null
    }

    if (message.length > MaxInputSize) {
      throw new IllegalArgumentException(s"Input too large: ${message.length} bytes")
    }

    try {
      val trip = mapper.readValue(new String(message, StandardCharsets.UTF_8), classOf[NYCTrip])
      validate(trip)
      trip
    } catch {
      case e: IllegalArgumentException => throw e
      case e: Exception =>
        throw new RuntimeException(s"Failed to deserialize NYCTrip: ${e.getMessage}", e)
    }
  }

  override def isEndOfStream(nextElement: NYCTrip): Boolean = false

  override def getProducedType: TypeInformation[NYCTrip] =
    TypeInformation.of(classOf[NYCTrip])

  /**
   * Business rule validation, kept from the previous string-based deserializer so
   * that malformed values are rejected at the edge rather than propagating into
   * the multiplier.
   */
  private def validate(trip: NYCTrip): Unit = {
    if (trip.tripId < 0) {
      throw new IllegalArgumentException(s"Invalid tripId: ${trip.tripId} (must be positive)")
    }
    if (trip.timestamp <= 0) {
      throw new IllegalArgumentException(s"Invalid timestamp: ${trip.timestamp} (must be positive)")
    }
    if (trip.distance < 0 || trip.distance > 10000) {
      throw new IllegalArgumentException(s"Invalid distance: ${trip.distance} (must be between 0 and 10000)")
    }
    if (trip.amount < 0 || trip.amount > 100000) {
      throw new IllegalArgumentException(s"Invalid amount: ${trip.amount} (must be between 0 and 100000)")
    }
    if (trip.fromLocationName == null || trip.fromLocationName.trim.isEmpty ||
        trip.toLocationName == null || trip.toLocationName.trim.isEmpty) {
      throw new IllegalArgumentException("Location names cannot be empty")
    }
    if (trip.category == null || trip.category.trim.isEmpty) {
      throw new IllegalArgumentException("Category cannot be empty")
    }
  }
}
