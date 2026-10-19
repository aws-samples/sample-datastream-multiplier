package com.amazonaws.services.msf

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Properties

/**
 * Validation tests for RuntimeConfig, covering the multipliedStream property
 * group's presence, type, range, and supported-value checks, plus the
 * seed-count warning for configurations with fewer seeds than copies.
 */
class RuntimeConfigSpec extends AnyFlatSpec with Matchers {

  private def validProperties(overrides: (String, String)*): Properties = {
    val props = new Properties()
    props.setProperty("multiple.factor", "3")
    props.setProperty("key.fields", "tripId")
    props.setProperty("random.seeds", "1000,2000")
    props.setProperty("updatable.fields", "distance,amount")
    props.setProperty("rate.limit.per.second", "500")
    props.setProperty("source.type", "kinesis")
    overrides.foreach { case (k, v) => props.setProperty(k, v) }
    props
  }

  "RuntimeConfig.validate" should "accept a fully valid property set" in {
    val result = RuntimeConfig.validate(validProperties())
    result.multiplier shouldBe 3
    result.keyFields shouldBe Set("tripId")
    result.randomSeeds shouldBe List(1000L, 2000L)
    result.updatableFields shouldBe Set("distance", "amount")
    result.rateLimitPerSecond shouldBe 500L
    result.sourceType shouldBe "kinesis"
  }

  it should "reject multiple.factor below the minimum" in {
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(validProperties("multiple.factor" -> "0"))
    }
    ex.getMessage should include("multiple.factor")
  }

  it should "reject multiple.factor above the maximum" in {
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(validProperties("multiple.factor" -> "101"))
    }
    ex.getMessage should include("multiple.factor")
  }

  it should "reject a non-numeric multiple.factor" in {
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(validProperties("multiple.factor" -> "abc"))
    }
    ex.getMessage should include("multiple.factor")
  }

  it should "reject key.fields referencing an unsupported field" in {
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(validProperties("key.fields" -> "tripId,notAField"))
    }
    ex.getMessage should include("key.fields")
    ex.getMessage should include("notAField")
  }

  it should "reject updatable.fields naming a field that is never mutated (tripId, timestamp)" in {
    Seq("tripId", "timestamp").foreach { field =>
      val ex = intercept[IllegalArgumentException] {
        RuntimeConfig.validate(validProperties("updatable.fields" -> s"distance,$field"))
      }
      ex.getMessage should include("updatable.fields")
      ex.getMessage should include(field)
    }
  }

  it should "reject updatable.fields referencing an unsupported field" in {
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(validProperties("updatable.fields" -> "bogusField"))
    }
    ex.getMessage should include("updatable.fields")
    ex.getMessage should include("bogusField")
  }

  it should "reject an empty key.fields" in {
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(validProperties("key.fields" -> ""))
    }
    ex.getMessage should include("key.fields")
  }

  it should "reject random.seeds with a non-numeric token" in {
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(validProperties("random.seeds" -> "1000,notanumber"))
    }
    ex.getMessage should include("random.seeds")
  }

  it should "reject an empty random.seeds" in {
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(validProperties("random.seeds" -> ""))
    }
    ex.getMessage should include("random.seeds")
  }

  it should "report only the per-token errors, not a duplicate 'no seeds' error, " +
    "when every token is non-numeric" in {
    // When random.seeds is "abc,def", randomSeedsTokens is non-empty but
    // randomSeeds ends up empty (both tokens fail to parse). The generic
    // "must contain at least one numeric seed" error should not also fire
    // on top of the two specific per-token errors, since two seeds were in
    // fact provided (just invalid ones) - firing both would be a confusing,
    // redundant message.
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(validProperties("random.seeds" -> "abc,def"))
    }
    ex.getMessage should include("non-numeric value: 'abc'")
    ex.getMessage should include("non-numeric value: 'def'")
    ex.getMessage should not include "must contain at least one numeric seed"
  }

  it should "reject a non-numeric rate.limit.per.second" in {
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(validProperties("rate.limit.per.second" -> "fast"))
    }
    ex.getMessage should include("rate.limit.per.second")
  }

  it should "reject an unsupported source.type" in {
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(validProperties("source.type" -> "firehose"))
    }
    ex.getMessage should include("source.type")
  }

  it should "report every problem at once instead of failing on the first" in {
    val ex = intercept[IllegalArgumentException] {
      RuntimeConfig.validate(
        validProperties(
          "multiple.factor" -> "0",
          "key.fields" -> "notAField",
          "source.type" -> "bogus"
        )
      )
    }
    ex.getMessage should include("multiple.factor")
    ex.getMessage should include("key.fields")
    ex.getMessage should include("source.type")
  }

  it should "accept fewer seeds than multiple.factor - 1 without throwing" in {
    // multiple.factor=10 but only 2 seeds is valid configuration, just lower
    // entropy per copy (see StreamMultiplierSpec for how computeEventHash
    // still keeps copies distinct) - this should only warn, not throw.
    noException should be thrownBy {
      RuntimeConfig.validate(validProperties("multiple.factor" -> "10", "random.seeds" -> "1000,2000"))
    }
  }
}
