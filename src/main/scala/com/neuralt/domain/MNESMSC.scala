package com.neuralt.domain

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import com.sksamuel.avro4s.{AvroName, AvroNamespace, AvroSchema}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import scala.util.Try

object MNESMSC {
  def apply(line:String, delim:Char): Try[MNESMSC] = {
    val cols = line.split(delim).map(_.trim)
    Try(
      new MNESMSC(cols(0),
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
        cols(14),
        LocalDateTime.parse(cols(15),DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toInstant(ZoneOffset.UTC).toEpochMilli,
        LocalDateTime.parse(cols(16),DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toInstant(ZoneOffset.UTC).toEpochMilli,
      )
    )
  }
}
@AvroName("mnesmsc")
@AvroNamespace("com.neuralt")
case class MNESMSC(@AvroName("entity") entity: String,
               @AvroName("starttime") eventTime: Long,
               @AvroName("destination") destination: String,
               @AvroName("duration") duration: Int,
               @AvroName("charge") charge: Float,
               @AvroName("scalltype") sCallType: String,
               @AvroName("scellid") sCellId: String,
               @AvroName("destinationcc") destinationCc: String,
               @AvroName("msc") msc: String,
               @AvroName("smsc") smsc: String,
               @AvroName("sphonetype") sPhoneType: String,
               @AvroName("srecordtype") sRecordType: String,
               @AvroName("susagetype") sUsageType: String,
               @AvroName("imsi") imsi: String,
               @AvroName("smarketsegment") sMarketSegment: String,
               @AvroName("stored") stored: Long,
               @AvroName("adjustmenttimestamp") adjustmentTimestamp: Long
              ) extends avroSeralizer {


  override def createAvro(): GenericData.Record = {
    val schema: Schema = AvroSchema[MNESMSC]
    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("entity", entity)
    avroRecord.put("starttime", eventTime)
    avroRecord.put("destination", destination)
    avroRecord.put("duration", duration)
    avroRecord.put("charge", charge)
    avroRecord.put("scalltype", sCallType)
    avroRecord.put("scellid", sCellId)
    avroRecord.put("destinationcc", destinationCc)
    avroRecord.put("msc", msc)
    avroRecord.put("smsc", smsc)
    avroRecord.put("sphonetype", sPhoneType)
    avroRecord.put("srecordtype", sRecordType)
    avroRecord.put("susagetype", sUsageType)
    avroRecord.put("imsi", imsi)
    avroRecord.put("smarketsegment", sMarketSegment)
    avroRecord.put("stored", stored)
    avroRecord.put("adjustmenttimestamp", adjustmentTimestamp)
    avroRecord
  }
}

