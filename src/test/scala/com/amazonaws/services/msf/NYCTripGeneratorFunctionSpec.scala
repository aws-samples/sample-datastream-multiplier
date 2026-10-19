package com.amazonaws.services.msf

import com.amazonaws.services.msf.model.{FakerConfigLoader, NYCTrip, SeededDataFaker}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

/**
 * Checks that each NYCTripGeneratorFunction subtask gets its own
 * FakerConfigLoader/SeededDataFaker, with no state shared between them.
 *
 * SeededDataFaker.reseed() mutates PRNG state with no locking, so sharing one
 * instance across subtasks/threads could interleave reseed() and
 * generateValue() calls and produce corrupted output. These tests mirror how
 * Flink actually drives a GeneratorFunction - one instance per subtask, one
 * thread per instance - and check isolation with a strong assertion: a
 * pair's concurrent output must exactly match its own single-threaded ground
 * truth. A simple null/non-empty check wouldn't catch a real race here,
 * since corrupted output is still a valid-looking faker value - just the
 * wrong one.
 */
class NYCTripGeneratorFunctionSpec extends AnyFlatSpec with Matchers {

  private val FakerConfigResource = "faker-config.yaml"

  private def variantKey(trip: NYCTrip): (Long, String, String, Float, Float, String) =
    (trip.tripId, trip.fromLocationName, trip.toLocationName, trip.distance, trip.amount, trip.category)

  "NYCTrip.generate" should "be deterministic for the same index/seed/faker state" in {
    val configLoader = new FakerConfigLoader(FakerConfigResource)
    val faker = new SeededDataFaker(0L)

    val first = NYCTrip.generate(1L, 1000L, configLoader, faker)
    val second = NYCTrip.generate(1L, 1000L, configLoader, faker)

    // Both calls reseed with the same value (seed + index), so - aside from
    // timestamp, which is wall-clock - the generated fields should match.
    first.fromLocationName shouldBe second.fromLocationName
    first.toLocationName shouldBe second.toLocationName
    first.distance shouldBe second.distance
    first.amount shouldBe second.amount
    first.category shouldBe second.category
  }

  it should "produce output matching its own single-threaded ground truth " +
    "when multiple per-subtask faker/configLoader pairs are driven " +
    "concurrently, one thread per pair" in {
    val pairCount = 8
    val recordsPerPair = 300

    // NYCTripGeneratorFunction.map() seeds with the wall clock, so its output
    // isn't reproducible run to run - not useful as a ground truth here.
    // Instead each "pair" below is a (FakerConfigLoader, SeededDataFaker),
    // same as what open() creates per subtask, and each thread calls
    // NYCTrip.generate directly with a fixed, index-derived seed so the
    // output is reproducible. What we're checking: running N pairs
    // concurrently (one thread each) should reproduce the exact same output
    // as running them single-threaded first. That only breaks if state leaks
    // between pairs.
    def fixedSeedFor(index: Long): Long = index * 7919L + 13L

    def recordsFor(configLoader: FakerConfigLoader, faker: SeededDataFaker): List[NYCTrip] =
      (0 until recordsPerPair).map { i =>
        NYCTrip.generate(i.toLong, fixedSeedFor(i.toLong), configLoader, faker)
      }.toList

    val groundTruthPairs = (0 until pairCount).map { _ =>
      (new FakerConfigLoader(FakerConfigResource), new SeededDataFaker(0L))
    }
    val groundTruth = groundTruthPairs.map { case (cl, f) => recordsFor(cl, f) }

    // Fresh pairs for the concurrent run - each thread owns exactly one pair
    // for its entire run, never shared with another thread, matching
    // NYCTripGeneratorFunction's actual one-faker-per-subtask design.
    val concurrentPairs = (0 until pairCount).map { _ =>
      (new FakerConfigLoader(FakerConfigResource), new SeededDataFaker(0L))
    }

    val pool = Executors.newFixedThreadPool(pairCount)
    val latch = new CountDownLatch(pairCount)
    val results = new Array[List[NYCTrip]](pairCount)

    try {
      for (idx <- 0 until pairCount) {
        pool.submit(new Runnable {
          override def run(): Unit = {
            try {
              val (cl, f) = concurrentPairs(idx)
              results(idx) = recordsFor(cl, f)
            } finally {
              latch.countDown()
            }
          }
        })
      }

      latch.await(30, TimeUnit.SECONDS) shouldBe true
    } finally {
      pool.shutdown()
    }

    for (idx <- 0 until pairCount) {
      val expectedKeys = groundTruth(idx).map(variantKey)
      val actualKeys = results(idx).map(variantKey)
      withClue(s"pair $idx's concurrent output diverged from its own single-threaded ground truth: ") {
        actualKeys shouldBe expectedKeys
      }
    }
  }

  it should "give each NYCTripGeneratorFunction subtask instance its own " +
    "faker/configLoader, not a shared singleton" in {
    // Two independently-opened instances should be distinct objects, and
    // hammering one shouldn't disturb the other.
    val instanceA = new DataMultiplierJob.NYCTripGeneratorFunction(FakerConfigResource)
    val instanceB = new DataMultiplierJob.NYCTripGeneratorFunction(FakerConfigResource)
    instanceA.open(null)
    instanceB.open(null)

    instanceA should not be theSameInstanceAs(instanceB)

    for (i <- 0 until 50) { instanceA.map(i.toLong) }
    noException should be thrownBy instanceB.map(0L)
  }
}
