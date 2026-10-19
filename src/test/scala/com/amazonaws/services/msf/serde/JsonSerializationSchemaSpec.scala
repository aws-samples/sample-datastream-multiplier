package com.amazonaws.services.msf.serde

import com.amazonaws.services.msf.model.NYCTrip
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

/**
 * Serializer tests for JsonSerializationSchema, including a round-trip
 * through JsonDeserializationSchema to confirm serialization stays
 * consistent with NYCTrip's mutable Flink POJO representation.
 */
class JsonSerializationSchemaSpec extends AnyFlatSpec with Matchers {

  private val serializer = new JsonSerializationSchema()
  private val deserializer = new JsonDeserializationSchema()

  "serialize" should "produce JSON containing all NYCTrip fields" in {
    val trip = NYCTrip(
      tripId = 5L,
      timestamp = 1726327891000L,
      fromLocationName = "Brooklyn - Williamsburg",
      toLocationName = "LaGuardia Airport",
      distance = 8.25f,
      amount = 30.5f,
      category = "2"
    )

    val json = new String(serializer.serialize(trip), StandardCharsets.UTF_8)

    json should include("\"tripId\":5")
    json should include("\"timestamp\":1726327891000")
    json should include("\"fromLocationName\":\"Brooklyn - Williamsburg\"")
    json should include("\"toLocationName\":\"LaGuardia Airport\"")
    json should include("\"category\":\"2\"")
  }

  it should "round-trip through JsonDeserializationSchema unchanged" in {
    val original = NYCTrip(
      tripId = 99L,
      timestamp = 1726327891000L,
      fromLocationName = "Queens - Astoria",
      toLocationName = "Newark Airport",
      distance = 15.0f,
      amount = 55.0f,
      category = "3"
    )

    val bytes = serializer.serialize(original)
    val roundTripped = deserializer.deserialize(bytes)

    roundTripped.tripId shouldBe original.tripId
    roundTripped.timestamp shouldBe original.timestamp
    roundTripped.fromLocationName shouldBe original.fromLocationName
    roundTripped.toLocationName shouldBe original.toLocationName
    roundTripped.distance shouldBe original.distance
    roundTripped.amount shouldBe original.amount
    roundTripped.category shouldBe original.category
    roundTripped shouldBe original
  }
}
