package com.neuralt.domain

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import com.sksamuel.avro4s.{AvroName, AvroNamespace, AvroSchema}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

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
case class MNE(@AvroName("custkey") entity: String,
               @AvroName("servedmsisdn") servedMSISDN: String,
               @AvroName("servedimsi") servedIMSI: String,
               @AvroName("tariffmodel") tariffModel: String,
               @AvroName("starttime") eventTime: Long,
               @AvroName("servicecode") serviceCode: String,
               @AvroName("eventclass") eventClass: String,
               @AvroName("servingnodeplmn") servingNodePLMN: String,
               @AvroName("apn") apn: String,
               @AvroName("duration") duration: Int,
               @AvroName("uplink") uplink: Float,
               @AvroName("downlink") downlink: Float,
               @AvroName("volume") volume: Float,
               @AvroName("cellid") cellID: Long,
               @AvroName("loc") loc: Long,
               @AvroName("gci") gci: String,
               @AvroName("rattype") ratType: Long,
               @AvroName("causeforrecclosing") causeForRecClosing: Long,
               @AvroName("chargingrulebasename") chargingRuleBaseName: String,
               @AvroName("ratinggroup") ratingGroup: Long,
               @AvroName("chargingid") chargingID: String,
               @AvroName("resource") resource: String,
               @AvroName("priceplan") pricePlan: String,
               @AvroName("chargedunits") chargedUnits: Float,
               @AvroName("roundedamount") roundedAmount: Float
              ) extends avroSeralizer {

  override def createAvro(): GenericData.Record = {
    val schema: Schema = AvroSchema[MNE]
    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("custkey", entity)
    avroRecord.put("servedmsisdn", servedMSISDN)
    avroRecord.put("servedimsi", servedIMSI)
    avroRecord.put("tariffmodel", tariffModel)
    avroRecord.put("starttime", eventTime)
    avroRecord.put("servicecode", serviceCode)
    avroRecord.put("eventclass", eventClass)
    avroRecord.put("servingnodeplmn", servingNodePLMN)
    avroRecord.put("apn", apn)
    avroRecord.put("duration", duration)
    avroRecord.put("uplink", uplink)
    avroRecord.put("downlink", downlink)
    avroRecord.put("volume", volume)
    avroRecord.put("cellid", cellID)
    avroRecord.put("loc", loc)
    avroRecord.put("gci", gci)
    avroRecord.put("rattype", ratType)
    avroRecord.put("causeforrecclosing", causeForRecClosing)
    avroRecord.put("chargingrulebasename", chargingRuleBaseName)
    avroRecord.put("ratinggroup", ratingGroup)
    avroRecord.put("chargingid", chargingID)
    avroRecord.put("resource", resource)
    avroRecord.put("priceplan", pricePlan)
    avroRecord.put("chargedunits", chargedUnits)
    avroRecord.put("roundedamount", roundedAmount)
    avroRecord
  }
}
