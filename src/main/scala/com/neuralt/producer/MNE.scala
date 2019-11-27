package com.neuralt.producer

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import com.sksamuel.avro4s.{AvroName, AvroNamespace, AvroSchema}
import org.apache.avro.generic.GenericData
import org.apache.avro.Schema

import scala.util.Try

object MNE {
  def apply(line:String, delim:Char): Try[MNE] = {
    val cols = line.split(delim).map(_.trim)
    Try(
      new MNE(cols(0),
        cols(1),
        cols(2),
        cols(3),
        LocalDateTime.parse(cols(4),DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toInstant(ZoneOffset.UTC).toEpochMilli,
        cols(5),
        cols(6),
        cols(7),
        cols(8),
        cols(9).toInt,
        cols(10).toFloat,
        cols(11).toFloat,
        cols(12).toFloat,
        cols(13).toLong,
        cols(14).toLong,
        cols(15),
        cols(16).toLong,
        cols(17).toLong,
        cols(18),
        cols(19).toLong,
        cols(20),
        cols(21),
        cols(22),
        cols(23).toFloat,
        cols(24).toFloat
      )
    )
  }
}
@AvroName("mne")
@AvroNamespace("com.neuralt")
case class MNE(@AvroName("custkey") CustKey: String,
               @AvroName("servedmsisdn") ServedMSISDN: String,
               @AvroName("servedimsi") ServedIMSI: String,
               @AvroName("tariffmodel") TariffModel: String,
               @AvroName("starttime") StartTime: Long,
               @AvroName("servicecode") ServiceCode: String,
               @AvroName("eventclass") EventClass: String,
               @AvroName("servingnodeplmn") ServingNodePLMN: String,
               @AvroName("apn") APN: String,
               @AvroName("duration") Duration: Int,
               @AvroName("uplink") Uplink: Float,
               @AvroName("downlink") Downlink: Float,
               @AvroName("volume") Volume: Float,
               @AvroName("cellid") CellID: Long,
               @AvroName("loc") LOC: Long,
               @AvroName("gci") GCI: String,
               @AvroName("rattype") RATType: Long,
               @AvroName("causeforrecclosing") CauseForRecClosing: Long,
               @AvroName("chargingrulebasename") ChargingRuleBaseName: String,
               @AvroName("ratinggroup") RatingGroup: Long,
               @AvroName("chargingid") ChargingID: String,
               @AvroName("resource") Resource: String,
               @AvroName("priceplan") PricePlan: String,
               @AvroName("chargedunits") ChargedUnits: Float,
               @AvroName("roundedamount") RoundedAmount: Float
              ) extends avroSeralizer {


  override def createAvro(): GenericData.Record = {
    val schema: Schema = AvroSchema[MNE]
    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("custkey", CustKey)
    avroRecord.put("servedmsisdn", ServedMSISDN)
    avroRecord.put("servedimsi", ServedIMSI)
    avroRecord.put("tariffmodel", TariffModel)
    avroRecord.put("starttime", StartTime)
    avroRecord.put("servicecode", ServiceCode)
    avroRecord.put("eventclass", EventClass)
    avroRecord.put("servingnodeplmn", ServingNodePLMN)
    avroRecord.put("apn", APN)
    avroRecord.put("duration", Duration)
    avroRecord.put("uplink", Uplink)
    avroRecord.put("downlink", Downlink)
    avroRecord.put("volume", Volume)
    avroRecord.put("cellid", CellID)
    avroRecord.put("loc", LOC)
    avroRecord.put("gci", GCI)
    avroRecord.put("rattype", RATType)
    avroRecord.put("causeforrecclosing", CauseForRecClosing)
    avroRecord.put("chargingrulebasename", ChargingRuleBaseName)
    avroRecord.put("ratinggroup", RatingGroup)
    avroRecord.put("chargingid", ChargingID)
    avroRecord.put("resource", Resource)
    avroRecord.put("priceplan", PricePlan)
    avroRecord.put("chargedunits", ChargedUnits)
    avroRecord.put("roundedamount", RoundedAmount)
    avroRecord
  }
}
