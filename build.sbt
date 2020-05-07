/*
 * Copyright 2020 Google LLC All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
organization := "com.google.cloud.imf"
name := "open-systems-connector"
version := "0.5.0-SNAPSHOT"
scalaVersion := "2.13.1"
publishMavenStyle := true

val exGuava = ExclusionRule(organization = "com.google.guava")

// Scala libraries
libraryDependencies ++= Seq(
  "com.github.scopt"  %% "scopt" % "3.7.1",
  "org.scalatest"     %% "scalatest" % "3.1.1" % Test
)

libraryDependencies ++= Seq("com.google.guava" % "guava" % "28.2-jre")

// Google libraries
libraryDependencies ++= Seq(
  "com.google.api-client"     % "google-api-client" % "1.30.9", // provided for google-cloud-bigquery
  "com.google.auto.value"     % "auto-value-annotations" % "1.7", // provided for google-cloud-bigquery
  "com.google.http-client"    % "google-http-client-apache-v2" % "1.34.2",
  "com.google.cloud"          % "google-cloud-bigquery" % "1.108.1",
  "com.google.cloud"          % "google-cloud-compute" % "0.117.0-alpha",
  "com.google.cloud"          % "google-cloud-storage" % "1.106.0",
  "com.google.protobuf"       % "protobuf-java" % "3.11.4",
  "com.google.protobuf"       % "protobuf-java-util" % "3.11.4"
).map(_ excludeAll exGuava)

// Apache libraries
libraryDependencies ++= Seq(
  "org.apache.hadoop"         % "hadoop-common" % "2.9.2", // provided for orc-core
  "org.apache.hadoop"         % "hadoop-hdfs-client" % "2.9.2", // provided for orc-core
  "org.apache.hive"           % "hive-storage-api" % "2.7.1",
  "org.apache.httpcomponents" % "httpclient" % "4.5.11",
  "org.apache.orc"            % "orc-core" % "1.6.2"
).map(_ excludeAll exGuava)

// SBT Assembly settings
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
mainClass in assembly := Some("com.google.cloud.imf.BqCsv")
test in assembly := Seq() // Don't run tests during assembly

// Compile settings
scalacOptions ++= Seq(
  "-opt:l:inline",
  "-opt-inline-from:**",
  "-deprecation",
  "-opt-warnings"
)

resourceGenerators in Compile += Def.task {
  val file = (resourceDirectory in Compile).value / "osc_build.txt"
  val fmt = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  val timestamp = fmt.format(new java.util.Date)
  IO.write(file, timestamp)
  Seq(file)
}.taskValue
