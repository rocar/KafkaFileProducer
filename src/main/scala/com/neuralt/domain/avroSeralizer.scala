package com.neuralt.domain

import org.apache.avro.generic.GenericData

trait avroSeralizer {
  val entity :String
  val eventTime: Long
  def createAvro(): GenericData.Record
}
