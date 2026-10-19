package com.amazonaws.services.msf.model

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Verifies NYCTrip satisfies Flink's POJO requirements: a public no-arg
 * constructor, public getter/setter pairs for every field, and recognition
 * by Flink's type extractor as a true PojoTypeInfo rather than falling back
 * to the generic Kryo-based GenericTypeInfo.
 */
class NYCTripSpec extends AnyFlatSpec with Matchers {

  "Flink's TypeExtractor" should "recognize NYCTrip as a POJO, not a generic/Kryo type" in {
    val typeInfo: TypeInformation[NYCTrip] = TypeInformation.of(classOf[NYCTrip])
    typeInfo shouldBe a[PojoTypeInfo[_]]
  }

  "NYCTrip" should "support a public no-arg constructor" in {
    val trip = new NYCTrip()
    trip.getTripId shouldBe 0L
    trip.getFromLocationName shouldBe ""
  }

  it should "expose working getters and setters for every field" in {
    val trip = new NYCTrip()
    trip.setTripId(10L)
    trip.setTimestamp(123L)
    trip.setFromLocationName("A")
    trip.setToLocationName("B")
    trip.setDistance(1.5f)
    trip.setAmount(2.5f)
    trip.setCategory("1")

    trip.getTripId shouldBe 10L
    trip.getTimestamp shouldBe 123L
    trip.getFromLocationName shouldBe "A"
    trip.getToLocationName shouldBe "B"
    trip.getDistance shouldBe 1.5f
    trip.getAmount shouldBe 2.5f
    trip.getCategory shouldBe "1"
  }

  it should "support construction via the all-args apply factory" in {
    val trip = NYCTrip(1L, 2L, "A", "B", 1.0f, 2.0f, "cat")
    trip.tripId shouldBe 1L
    trip.category shouldBe "cat"
  }

  it should "support copy() with partial field overrides" in {
    val original = NYCTrip(1L, 2L, "A", "B", 1.0f, 2.0f, "cat")
    val updated = original.copy(distance = 99.0f)

    updated.distance shouldBe 99.0f
    // Unmodified fields stay the same
    updated.tripId shouldBe original.tripId
    updated.fromLocationName shouldBe original.fromLocationName
  }

  it should "implement value equality via equals/hashCode" in {
    val a = NYCTrip(1L, 2L, "A", "B", 1.0f, 2.0f, "cat")
    val b = NYCTrip(1L, 2L, "A", "B", 1.0f, 2.0f, "cat")
    val c = NYCTrip(1L, 2L, "A", "B", 1.0f, 2.0f, "different")

    a shouldEqual b
    a.hashCode shouldEqual b.hashCode
    a should not equal c
  }
}
