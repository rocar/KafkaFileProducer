name := "KafkaFileProducer"
version := "0.1"

scalaVersion := "2.12.10"
resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
resolvers += "confluent.io" at "http://packages.confluent.io/maven/"

val logback = "1.2.3"
val confluentVersion = "5.2.2"
val kafkaVersion = "2.3.0"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"

libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-core" % "3.0.2"
libraryDependencies += "org.apache.avro" % "avro" % "1.9.1"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % confluentVersion

libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % logback
