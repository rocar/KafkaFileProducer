package com.neuralt.producer

import org.apache.avro.generic.GenericData

trait avroSeralizer {
  def createAvro(): GenericData.Record
}
