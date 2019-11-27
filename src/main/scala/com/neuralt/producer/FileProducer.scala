package com.neuralt.producer

import java.io.{FileNotFoundException, IOException, _}
import java.util.zip._
import java.util.{Properties, UUID}

import com.neuralt.domain._
import com.sksamuel.avro4s.AvroSchema
import com.typesafe.config.ConfigFactory
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.{Failure, Success, Try}

object FileProducer extends App {
  private val log = LoggerFactory.getLogger(FileProducer.getClass)

  if (args.length < 2) {
    println(s"Usage: <MNE/MNESMSC/MNEMSC> <filename> <msg/s>")
    System.exit(0)
  }

  // Application config
  lazy private val config = ConfigFactory.load()
  val brokerAddress = config.getString("bootstrapServers")
  val schemaRegistryUrl = config.getString("schemaRegistry.url")
  val topicName = config.getString("outTopic")
  val applicationID = config.getString("applicationID")
  log.info("Config: " + config)

  val schema = args(0) match {
    case "MNE" => AvroSchema[MNE]
    case "MNESMSC" => AvroSchema[MNESMSC]
    case "MNEMSC" => AvroSchema[MNESMSC]
    case _ => { log.info("Unknown type: " + args(0)); System.exit(0)}
  }

  val filename = args(1)
  val rate = if(Try(args(2).toInt).isSuccess) args(1).toInt else 0

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getCanonicalName)
  props.put(ProducerConfig.ACKS_CONFIG, "0")
  props.put(ProducerConfig.RETRIES_CONFIG, "0")
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
  props.put(ProducerConfig.LINGER_MS_CONFIG, "1")
  props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432")
  props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl)

  val producer = new KafkaProducer[String, GenericRecord](props)
  log.info("Process file: " + filename)
  log.info("At rate: " + rate)
  log.info("AvroSchema: " + schema)
  var produced = 0
  var skipped = 0
  var elapsed = 0.00001F
  val t0 = System.currentTimeMillis()

  val bufferedSource = Source.fromInputStream(new GZIPInputStream(new FileInputStream(filename)))
  try {
    for (line <- bufferedSource.getLines) {
      log.debug(line)

      elapsed = (System.currentTimeMillis() - t0).toInt
      while ((produced/elapsed)*1000 > rate && rate > 0) {
        Thread.sleep(100)
        elapsed = (System.currentTimeMillis() - t0).toInt
      }

      MNE(line, '|') match {
        case Success(record) => {
          producer.send(new ProducerRecord[String, GenericRecord](topicName,
            null,
            record.eventTime,
            record.entity,
            record.createAvro()))
          produced += 1
          print(".")
        }
        case Failure(exception) => {
          log.error("Error line with Exception: " + exception + " [" + line + "]")
          skipped += 1
        }
      }
    }
  } catch {
    case e: FileNotFoundException => println("Couldn't find that file.")
    case e: IOException => println("Got an IOException!")
    case unknown: Throwable => println("Unknown exception:" + unknown)
  } finally {
    print("\n")
    log.info("Total: " + (produced + skipped))
    log.info("Produced: " + produced)
    log.info("Skipped: " + skipped)
    log.info("Elapsed(s): " + elapsed)
    producer.flush()
    producer.close()
    bufferedSource.close()
  }
}
