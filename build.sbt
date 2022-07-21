name := "spark-playground"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.2"
val vegasVersion = "0.3.11"
val postgresVersion = "42.2.2"
val cassandraConnectorVersion = "3.0.0"
val log4jVersion = "2.4.1"
val twitter4jVersion = "4.0.7"
val kafkaVersion = "2.4.0"
val akkaHttpVersion = "10.1.7"
val akkaVersion = "2.5.24"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.spark" %% "spark-streaming" % sparkVersion,

  // streaming-kafka
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,

  // low-level integrations
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  // kafka
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,

  // akka
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,

  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectorVersion,

  // postgres
  "org.postgresql" % "postgresql" % postgresVersion,

  // twitter
  "org.twitter4j" % "twitter4j-core" % twitter4jVersion,
  "org.twitter4j" % "twitter4j-stream" % twitter4jVersion,

  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion
)

