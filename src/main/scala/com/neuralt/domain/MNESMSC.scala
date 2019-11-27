package com.neuralt.domain

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import com.neuralt.producer.avroSeralizer
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
case class MNESMSC(@AvroName("entity") Entity: String,
               @AvroName("starttime") StartTime: Long,
               @AvroName("destination") Destination: String,
               @AvroName("duration") Duration: Int,
               @AvroName("charge") Charge: Float,
               @AvroName("scalltype") SCallType: String,
               @AvroName("scellid") SCellId: String,
               @AvroName("destinationcc") DestinationCc: String,
               @AvroName("msc") Msc: String,
               @AvroName("smsc") Smsc: String,
               @AvroName("sphonetype") SPhoneType: String,
               @AvroName("srecordtype") SRecordType: String,
               @AvroName("susagetype") SUsageType: String,
               @AvroName("imsi") Imsi: String,
               @AvroName("smarketsegment") SMarketSegment: String,
               @AvroName("stored") Stored: Long,
               @AvroName("adjustmenttimestamp") AdjustmentTimestamp: Long
              ) extends avroSeralizer {


  override def createAvro(): GenericData.Record = {
    val schema: Schema = AvroSchema[MNESMSC]
    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("entity", Entity)
    avroRecord.put("starttime", StartTime)
    avroRecord.put("destination", Destination)
    avroRecord.put("duration", Duration)
    avroRecord.put("charge", Charge)
    avroRecord.put("scalltype", SCallType)
    avroRecord.put("scellid", SCellId)
    avroRecord.put("destinationcc", DestinationCc)
    avroRecord.put("msc", Msc)
    avroRecord.put("smsc", Smsc)
    avroRecord.put("sphonetype", SPhoneType)
    avroRecord.put("srecordtype", SRecordType)
    avroRecord.put("susagetype", SUsageType)
    avroRecord.put("imsi", Imsi)
    avroRecord.put("smarketsegment", SMarketSegment)
    avroRecord.put("stored", Stored)
    avroRecord.put("adjustmenttimestamp", AdjustmentTimestamp)
    avroRecord
  }
}

