package com.amazonaws.services.msf.serde

import com.amazonaws.services.msf.model.NYCTrip
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.flink.api.common.serialization.SerializationSchema

import java.nio.charset.StandardCharsets

/**
 * Writes NYCTrip records to the sink stream as one JSON object per record.
 *
 * The output shape matches what JsonDeserializationSchema reads, so the amplified
 * stream can be fed straight back into another instance of this application, or
 * into any consumer that expects JSON.
 */
class JsonSerializationSchema extends SerializationSchema[NYCTrip] {

  // ObjectMapper is not serializable, so it is rebuilt on each task manager.
  @transient private lazy val mapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m
  }

  override def serialize(element: NYCTrip): Array[Byte] =
    mapper.writeValueAsString(element).getBytes(StandardCharsets.UTF_8)
}
