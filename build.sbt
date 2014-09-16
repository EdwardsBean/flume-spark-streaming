name := "flume-spark-streaming"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.0.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming-flume" % "1.0.0"

libraryDependencies += "redis.clients" % "jedis" % "2.4.2"

