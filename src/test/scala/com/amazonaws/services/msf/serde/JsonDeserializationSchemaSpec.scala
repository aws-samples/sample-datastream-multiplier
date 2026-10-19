package com.amazonaws.services.msf.serde

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

/**
 * JSON boundary case tests for JsonDeserializationSchema: message size
 * limits, numeric bounds, and required-field checks. Documents the
 * protections already in place (size limit, numeric bounds, non-empty
 * checks) distinct from what the README's security warning calls out as
 * still missing (field length / character-set validation).
 */
class JsonDeserializationSchemaSpec extends AnyFlatSpec with Matchers {

  private def schema = new JsonDeserializationSchema()

  private def json(
      tripId: Long = 1L,
      timestamp: Long = 1726327891000L,
      fromLocationName: String = "Manhattan - Times Square",
      toLocationName: String = "JFK Airport",
      distance: Double = 12.5,
      amount: Double = 42.75,
      category: String = "1"
  ): String =
    s"""{"tripId":$tripId,"timestamp":$timestamp,"fromLocationName":"$fromLocationName",""" +
      s""""toLocationName":"$toLocationName","distance":$distance,"amount":$amount,"category":"$category"}"""

  "deserialize" should "parse a well-formed record" in {
    val trip = schema.deserialize(json().getBytes(StandardCharsets.UTF_8))
    trip.tripId shouldBe 1L
    trip.fromLocationName shouldBe "Manhattan - Times Square"
    trip.category shouldBe "1"
  }

  it should "return null for a null message" in {
    schema.deserialize(null) shouldBe null
  }

  it should "return null for an empty message" in {
    schema.deserialize(Array.emptyByteArray) shouldBe null
  }

  it should "ignore unknown fields instead of failing" in {
    val withExtra =
      """{"tripId":1,"timestamp":1726327891000,"fromLocationName":"A","toLocationName":"B",""" +
        """"distance":1.0,"amount":1.0,"category":"1","unexpectedField":"shouldBeIgnored"}"""
    noException should be thrownBy schema.deserialize(withExtra.getBytes(StandardCharsets.UTF_8))
  }

  it should "reject a message over the 100 KB size limit" in {
    val oversized = "x" * (102400 + 1)
    an[IllegalArgumentException] should be thrownBy
      schema.deserialize(oversized.getBytes(StandardCharsets.UTF_8))
  }

  it should "accept a message exactly at the 100 KB boundary (parse failure, not size rejection)" in {
    // A 100 KB message of padding is not valid JSON, so this should fail to
    // parse - but it must fail with the JSON parse error, not the size-limit
    // IllegalArgumentException, confirming the boundary itself is inclusive.
    val atLimit = ("x" * 102400)
    val ex = intercept[RuntimeException] {
      schema.deserialize(atLimit.getBytes(StandardCharsets.UTF_8))
    }
    ex.getMessage should include("Failed to deserialize")
  }

  it should "reject a negative tripId" in {
    an[IllegalArgumentException] should be thrownBy
      schema.deserialize(json(tripId = -1L).getBytes(StandardCharsets.UTF_8))
  }

  it should "accept tripId of exactly 0 (lower boundary)" in {
    noException should be thrownBy schema.deserialize(json(tripId = 0L).getBytes(StandardCharsets.UTF_8))
  }

  it should "reject a non-positive timestamp" in {
    an[IllegalArgumentException] should be thrownBy
      schema.deserialize(json(timestamp = 0L).getBytes(StandardCharsets.UTF_8))
  }

  it should "reject a negative distance" in {
    an[IllegalArgumentException] should be thrownBy
      schema.deserialize(json(distance = -0.1).getBytes(StandardCharsets.UTF_8))
  }

  it should "reject a distance over the 10000 upper bound" in {
    an[IllegalArgumentException] should be thrownBy
      schema.deserialize(json(distance = 10000.1).getBytes(StandardCharsets.UTF_8))
  }

  it should "accept distance of exactly 10000 (upper boundary)" in {
    noException should be thrownBy schema.deserialize(json(distance = 10000.0).getBytes(StandardCharsets.UTF_8))
  }

  it should "reject an amount over the 100000 upper bound" in {
    an[IllegalArgumentException] should be thrownBy
      schema.deserialize(json(amount = 100000.1).getBytes(StandardCharsets.UTF_8))
  }

  it should "reject an empty fromLocationName" in {
    an[IllegalArgumentException] should be thrownBy
      schema.deserialize(json(fromLocationName = "").getBytes(StandardCharsets.UTF_8))
  }

  it should "reject an empty category" in {
    an[IllegalArgumentException] should be thrownBy
      schema.deserialize(json(category = "").getBytes(StandardCharsets.UTF_8))
  }

  it should "throw a RuntimeException for malformed JSON" in {
    an[RuntimeException] should be thrownBy
      schema.deserialize("{not valid json".getBytes(StandardCharsets.UTF_8))
  }
}
