package com.amazonaws.services.msf.process

import com.amazonaws.services.msf.model.NYCTrip
import org.apache.flink.streaming.api.operators.StreamFlatMap
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Properties
import scala.collection.JavaConverters._

/**
 * Cardinality and uniqueness tests for StreamMultiplier: multiple.factor
 * copies in, multiple.factor outputs out, and no two copies of the same
 * input share identical randomized field values.
 */
class StreamMultiplierSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val FakerConfigResource = "faker-config.yaml"

  private def harnessFor(properties: Properties): OneInputStreamOperatorTestHarness[NYCTrip, NYCTrip] = {
    val multiplier = new StreamMultiplier(FakerConfigResource, properties)
    val operator = new StreamFlatMap[NYCTrip, NYCTrip](multiplier)
    val harness = new OneInputStreamOperatorTestHarness[NYCTrip, NYCTrip](operator)
    harness.open()
    harness
  }

  private def properties(
      multipleFactor: Int,
      keyFields: String = "tripId",
      randomSeeds: String = "1000,2000,3000,4000,5000",
      updatableFields: String = "fromLocationName,toLocationName,distance,amount,category",
      rateLimit: String = "1000",
      sourceType: String = "kinesis"
  ): Properties = {
    val props = new Properties()
    props.setProperty("multiple.factor", multipleFactor.toString)
    props.setProperty("key.fields", keyFields)
    props.setProperty("random.seeds", randomSeeds)
    props.setProperty("updatable.fields", updatableFields)
    props.setProperty("rate.limit.per.second", rateLimit)
    props.setProperty("source.type", sourceType)
    props
  }

  private def sampleTrip(tripId: Long): NYCTrip =
    NYCTrip(
      tripId = tripId,
      timestamp = 1726327891000L,
      fromLocationName = "Manhattan - Times Square",
      toLocationName = "JFK Airport",
      distance = 12.5f,
      amount = 42.75f,
      category = "1"
    )

  "StreamMultiplier" should "emit exactly multiple.factor records per input (cardinality)" in {
    val harness = harnessFor(properties(multipleFactor = 10))
    try {
      harness.processElement(sampleTrip(1L), 0L)
      harness.getOutput.size() shouldBe 10
    } finally {
      harness.close()
    }
  }

  it should "emit exactly 1 record when multiple.factor is 1 (passthrough)" in {
    val harness = harnessFor(properties(multipleFactor = 1))
    try {
      harness.processElement(sampleTrip(1L), 0L)
      harness.getOutput.size() shouldBe 1
    } finally {
      harness.close()
    }
  }

  it should "produce distinct output even when random.seeds has fewer entries than multiple.factor - 1" in {
    // With multiple.factor=10 but only 5 configured seeds, every copy index
    // still contributes its own entropy to computeEventHash, so copies do
    // not all collapse onto identical randomized output just because the
    // seed list ran out. 10 input records should still produce 10 output
    // records per input, and copies of a single input should not all
    // collapse onto the same randomized values.
    val harness = harnessFor(properties(multipleFactor = 10, randomSeeds = "1000,2000,3000,4000,5000"))
    try {
      harness.processElement(sampleTrip(1L), 0L)

      val outputs = harness.extractOutputValues().asScala.toList
      outputs should have size 10

      // The distinct (fromLocationName, toLocationName, distance, amount, category)
      // tuples across the 10 copies of this one input. Mixing the copy index
      // into the hash means copies do not collide deterministically just
      // because the seed list ran out.
      val distinctVariants = outputs.map { t =>
        (t.fromLocationName, t.toLocationName, t.distance, t.amount, t.category)
      }.distinct

      // Not every copy is guaranteed unique (finite faker domains, e.g.
      // category only has 3 possible values, can coincide by chance), but a
      // severe collapse (e.g. only 7 distinct out of 10, with one value
      // repeated 4x) should not happen.
      distinctVariants.size should be >= 8
    } finally {
      harness.close()
    }
  }

  it should "produce the same output deterministically across repeated runs with the same input and seeds" in {
    val runOutputs = (1 to 2).map { _ =>
      val harness = harnessFor(properties(multipleFactor = 5))
      try {
        harness.processElement(sampleTrip(42L), 0L)
        harness.extractOutputValues().asScala.toList.map(t =>
          (t.tripId, t.fromLocationName, t.toLocationName, t.distance, t.amount, t.category)
        )
      } finally {
        harness.close()
      }
    }

    runOutputs(0) shouldBe runOutputs(1)
  }

  it should "always emit the original, unmodified record first" in {
    val harness = harnessFor(properties(multipleFactor = 3))
    try {
      val original = sampleTrip(7L)
      harness.processElement(original, 0L)

      val outputs = harness.extractOutputValues().asScala.toList
      val first = outputs.head
      first.tripId shouldBe original.tripId
      first.fromLocationName shouldBe original.fromLocationName
      first.toLocationName shouldBe original.toLocationName
      first.distance shouldBe original.distance
      first.amount shouldBe original.amount
      first.category shouldBe original.category
    } finally {
      harness.close()
    }
  }

  it should "not emit anything for a null event" in {
    val harness = harnessFor(properties(multipleFactor = 5))
    try {
      harness.processElement(null.asInstanceOf[NYCTrip], 0L)
      harness.getOutput.size() shouldBe 0
    } finally {
      harness.close()
    }
  }
}
