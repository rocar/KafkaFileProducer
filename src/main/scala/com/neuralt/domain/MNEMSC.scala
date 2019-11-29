package com.neuralt.domain

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import com.sksamuel.avro4s.{AvroName, AvroNamespace, AvroSchema}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import scala.util.Try

object MNEMSC {
  def apply(line:String, delim:Char): Try[MNEMSC] = {
    val cols = line.split(delim).map(_.trim)
    Try(
      new MNEMSC(cols(0),
        LocalDateTime.parse(cols(1),DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toInstant(ZoneOffset.UTC).toEpochMilli,
        cols(2),
        cols(3).toInt,
        cols(4).toFloat,
        cols(5),
        cols(6),
        cols(7),
        cols(8),
        cols(9),
        cols(10),
        cols(11),
        cols(12),
        cols(13),
        LocalDateTime.parse(cols(14),DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toInstant(ZoneOffset.UTC).toEpochMilli,
        LocalDateTime.parse(cols(15) match {case "null" => "19700101000000"; case v => v},DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toInstant(ZoneOffset.UTC).toEpochMilli,
      )
    )
  }
}
@AvroName("mnemsc")
@AvroNamespace("com.neuralt")
case class MNEMSC(@AvroName("entity") entity: String,
                   @AvroName("starttime") eventTime: Long,
                   @AvroName("destination") destination: String,
                   @AvroName("duration") duration: Int,
                   @AvroName("charge") charge: Float,
                   @AvroName("calltype") callType: String,
                   @AvroName("cellid") cellId: String,
                   @AvroName("destinationcc") destinationCc: String,
                   @AvroName("msc") msc: String,
                   @AvroName("phonetype") phoneType: String,
                   @AvroName("recordtype") recordType: String,
                   @AvroName("usagetype") usageType: String,
                   @AvroName("imsi") imsi: String,
                   @AvroName("marketsegment") marketSegment: String,
                   @AvroName("stored") stored: Long,
                   @AvroName("adjustmenttimestamp") adjustmentTimestamp: Long
                  ) extends avroSeralizer {


  override def createAvro(): GenericData.Record = {
    val schema: Schema = AvroSchema[MNEMSC]
    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("entity", entity)
    avroRecord.put("starttime", eventTime)
    avroRecord.put("destination", destination)
    avroRecord.put("duration", duration)
    avroRecord.put("charge", charge)
    avroRecord.put("calltype", callType)
    avroRecord.put("cellid", cellId)
    avroRecord.put("destinationcc", destinationCc)
    avroRecord.put("msc", msc)
    avroRecord.put("phonetype", phoneType)
    avroRecord.put("recordtype", recordType)
    avroRecord.put("usagetype", usageType)
    avroRecord.put("imsi", imsi)
    avroRecord.put("marketsegment", marketSegment)
    avroRecord.put("stored", stored)
    avroRecord.put("adjustmenttimestamp", adjustmentTimestamp)
    avroRecord
  }
}

