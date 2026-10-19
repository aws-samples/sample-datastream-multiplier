package com.amazonaws.services.msf

import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties
import scala.util.Try

/**
 * Validates the `multipliedStream` runtime property group as a single unit
 * before any source/operator is built from it.
 *
 * `multiple.factor`, `key.fields`, `random.seeds`, `updatable.fields`,
 * `rate.limit.per.second`, and `source.type` are all validated together for
 * presence, type, range, and supported values. A bad value (or a typo'd
 * field name) is caught here with a clear message, rather than throwing a
 * late, confusing exception deep inside `StreamMultiplier` or being silently
 * ignored.
 *
 * All problems found are collected and reported together in one exception,
 * rather than failing fast on the first one, so an operator fixing runtime
 * properties doesn't have to redeploy once per mistake.
 */
object RuntimeConfig {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val MIN_MULTIPLIER: Int = 1
  val MAX_MULTIPLIER: Int = 100

  // The only NYCTrip fields that key.fields / updatable.fields may reference.
  // Keep in sync with the NYCTrip case class in model/EventObj.scala.
  val SupportedFields: Set[String] =
    Set("tripId", "timestamp", "fromLocationName", "toLocationName", "distance", "amount", "category")

  // The subset of SupportedFields that NYCTrip.randomizeFields can actually
  // mutate. tripId and timestamp are valid key fields but are always carried
  // through unchanged, so listing them in updatable.fields would otherwise be
  // silently ignored. Keep in sync with NYCTrip.randomizeFields.
  val UpdatableFields: Set[String] =
    Set("fromLocationName", "toLocationName", "distance", "amount", "category")

  val SupportedSourceTypes: Set[String] = Set("kinesis", "datagen")

  final case class Validated(
    multiplier: Int,
    keyFields: Set[String],
    randomSeeds: List[Long],
    updatableFields: Set[String],
    rateLimitPerSecond: Long,
    sourceType: String
  )

  /**
   * Validates every key read out of the `multipliedStream` property group.
   * Throws a single `IllegalArgumentException` listing every problem found,
   * or returns the parsed, validated configuration.
   */
  def validate(properties: Properties): Validated = {
    val errors = scala.collection.mutable.ListBuffer.empty[String]

    val multiplierStr = properties.getProperty("multiple.factor", "1").trim
    val multiplier = Try(multiplierStr.toInt).toOption.filter(_ > 0) match {
      case Some(m) if m >= MIN_MULTIPLIER && m <= MAX_MULTIPLIER => m
      case Some(m) =>
        errors += s"multiple.factor must be between $MIN_MULTIPLIER and $MAX_MULTIPLIER, got: $m"
        m
      case None =>
        errors += s"multiple.factor must be a valid positive integer, got: '$multiplierStr'"
        1
    }

    val keyFieldsRaw = properties.getProperty("key.fields", "")
    val keyFields = splitCsv(keyFieldsRaw)
    if (keyFields.isEmpty) {
      errors += "key.fields must contain at least one field name"
    }
    val unknownKeyFields = keyFields -- SupportedFields
    if (unknownKeyFields.nonEmpty) {
      errors += s"key.fields contains unsupported field(s): ${unknownKeyFields.mkString(", ")}. " +
        s"Supported fields: ${SupportedFields.mkString(", ")}"
    }

    val updatableFieldsRaw = properties.getProperty("updatable.fields", "")
    val updatableFields = splitCsv(updatableFieldsRaw)
    val unknownUpdatableFields = updatableFields -- UpdatableFields
    if (unknownUpdatableFields.nonEmpty) {
      errors += s"updatable.fields contains unsupported field(s): ${unknownUpdatableFields.mkString(", ")}. " +
        s"Supported fields: ${UpdatableFields.mkString(", ")}"
    }

    val randomSeedsRaw = properties.getProperty("random.seeds", "")
    val randomSeedsTokens = randomSeedsRaw.split(",").map(_.trim).filter(_.nonEmpty).toList
    val randomSeeds = randomSeedsTokens.flatMap { token =>
      Try(token.toLong).toOption.orElse {
        errors += s"random.seeds contains a non-numeric value: '$token'"
        None
      }
    }
    // Only fire this when there were no tokens at all (random.seeds was blank).
    // If tokens were present but all invalid, the per-token "non-numeric value"
    // errors above already cover it; adding this one too produced a confusing
    // double error (e.g. for "abc,def": two per-token errors plus a misleading
    // "no seeds" message, even though two tokens were in fact provided).
    if (randomSeedsTokens.isEmpty) {
      errors += "random.seeds must contain at least one numeric seed"
    }
    // Not an error: fewer seeds than (multiplier - 1) means seeds will be reused
    // across copies. computeEventHash still mixes in the copy index so output
    // stays distinct run-to-run, but it's worth flagging in logs since a finite
    // faker domain (e.g. category has 3 possible values) can still coincide.
    if (randomSeeds.nonEmpty && randomSeeds.size < multiplier - 1) {
      logger.warn(
        s"random.seeds has ${randomSeeds.size} seed(s) but multiple.factor is $multiplier " +
          s"(needs ${multiplier - 1} to give every copy a distinct seed). Seeds will be reused " +
          "across copies; copy index is still mixed into the hash so this does not reintroduce " +
          "guaranteed duplicates, but it does reduce entropy."
      )
    }

    val rateLimitStr = properties.getProperty("rate.limit.per.second", "100000").trim
    val rateLimitPerSecond = Try(rateLimitStr.toLong).toOption.filter(_ > 0) match {
      case Some(r) => r
      case None =>
        errors += s"rate.limit.per.second must be a valid positive integer, got: '$rateLimitStr'"
        1L
    }

    val sourceType = properties.getProperty("source.type", "kinesis").trim.toLowerCase
    if (!SupportedSourceTypes.contains(sourceType)) {
      errors += s"source.type must be one of ${SupportedSourceTypes.mkString(", ")}, got: '$sourceType'"
    }

    if (errors.nonEmpty) {
      throw new IllegalArgumentException(
        s"Invalid 'multipliedStream' runtime properties (${errors.size} problem(s)):\n" +
          errors.map(e => s"  - $e").mkString("\n")
      )
    }

    Validated(multiplier, keyFields, randomSeeds, updatableFields, rateLimitPerSecond, sourceType)
  }

  private def splitCsv(raw: String): Set[String] =
    raw.split(",").map(_.trim).filter(_.nonEmpty).toSet
}
