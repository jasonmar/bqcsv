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

package com.google.cloud.imf

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.{BigQuery, ExternalTableDefinition, JobId, JobInfo, StandardTableDefinition, TableId, TableInfo}
import com.google.cloud.imf.bqcsv.{BQ, BqCsvConfig, BqCsvConfigParser, CliSchemaProvider, GCS, Logging, NoOpMemoryManager, OrcAppender, SchemaProvider, SimpleGCSFileSystem, TableSchemaProvider, Util}
import com.google.cloud.storage.Storage
import com.google.rpc.{Code, Status}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.orc.{CompressionKind, OrcConf, OrcFile}

import scala.io.Source

object BqCsv extends Logging {
  def main(args: Array[String]): Unit = {
    BqCsvConfigParser.parse(args) match {
      case Some(cfg) =>
        Util.configureLogging(cfg.debug)
        val status = run(cfg)
        if (status.getCode != Code.OK_VALUE)
          System.exit(status.getCode)
      case _ =>
        System.exit(1)
    }
  }

  final val MegaByte: Long = 1024*1024*1024
  final val OptimalCompressBuffer: Int = 32*1024

  def run(cfg: BqCsvConfig): Status = {
    val src = Source.fromFile(cfg.source, "UTF-8")
    val credentials = GoogleCredentials.getApplicationDefault.createScoped(Util.Scopes)
    val bq: BigQuery = BQ.defaultClient(cfg.projectId, cfg.location, credentials)

    // Get Schema from BigQuery Table
    val templateTableId: TableId =
      if (cfg.templateTableSpec.nonEmpty) {
        logger.info(s"Getting schema from template table ${cfg.templateTableSpec}")
        BQ.resolveTableSpec(cfg.templateTableSpec,cfg.projectId,cfg.datasetId)
      } else {
        logger.info(s"Getting schema from destination table ${cfg.destTableSpec}")
        BQ.resolveTableSpec(cfg.destTableSpec,cfg.projectId,cfg.datasetId)
      }
    val table = Option(bq.getTable(templateTableId))
    val schema = table.map(_.getDefinition[StandardTableDefinition].getSchema)

    // Fall back to CLI schema if BigQuery Table doesn't exist
    val sp = schema.map(TableSchemaProvider(_)).getOrElse(CliSchemaProvider(cfg.schema))

    try {
      val lines = src.getLines
      val gcs: Storage = GCS.defaultClient(credentials)
      val uri = new java.net.URI(cfg.stagingUri)
      if (cfg.replace) GCS.delete(gcs, uri)
      else GCS.assertEmpty(gcs, uri)
      val rowCount = write(lines, cfg.partSizeMB*MegaByte, cfg.delimiter, uri, sp, gcs)
      logger.info(s"Wrote $rowCount rows")
    } finally {
      src.close
    }

    if (cfg.external){
      logger.info("Registering External Table")
      BQ.register(cfg, bq) match {
        case Some(tbl) =>
          val msg = s"Registered External Table\n$tbl"
          logger.info(msg)
          Status.newBuilder.setMessage(msg).setCode(Code.OK_VALUE).build
        case _ =>
          val msg = s"Failed to register External Table ${cfg.destTableSpec}"
          logger.info(msg)
          Status.newBuilder.setMessage(msg).setCode(Code.OK_VALUE).build
      }
    } else {
      // Submit Load job
      val loadJobConfig = BQ.configureLoadJob(cfg, schema)
      logger.info("Submitting load job")
      logger.debug(loadJobConfig)
      val jobId = JobId.of(s"bqcsv_load_${System.currentTimeMillis}")
      val job = bq.create(JobInfo.of(jobId, loadJobConfig))
      val completed = BQ.await(job, jobId, 3600)
      logger.info("Load job completed")
      logger.debug(completed)
      BQ.getStatus(completed) match {
        case Some(status) =>
          logger.info(s"Load job ${jobId.getJob} has status ${status.state}")
          if (status.hasError) {
            val msg =
              s"""Error:
                 |${status.error}
                 |${status.executionErrors.mkString("Execution Errors:\n","\n","")}""".stripMargin
            logger.error(msg)
            Status.newBuilder.setMessage(msg).setCode(Code.CANCELLED_VALUE).build
          } else {
            Status.newBuilder.setCode(Code.OK_VALUE).build
          }
        case _ =>
          Status.newBuilder.setMessage("missing status").setCode(Code.NOT_FOUND_VALUE).build
      }
    }
  }

  def write(lines: Iterator[String],
            partSize: Long,
            delimiter: Char,
            baseUri: java.net.URI,
            schemaProvider: SchemaProvider,
            gcs: Storage): Long = {
    val orcConfig = {
      val c = new Configuration(false)
      OrcConf.COMPRESS.setString(c, "ZLIB")
      OrcConf.COMPRESSION_STRATEGY.setString(c, "SPEED")
      OrcConf.ENABLE_INDEXES.setBoolean(c, false)
      OrcConf.OVERWRITE_OUTPUT_FILE.setBoolean(c, true)
      OrcConf.MEMORY_POOL.setDouble(c, 0.5d)
      c
    }

    val stats = new FileSystem.Statistics(SimpleGCSFileSystem.Scheme)

    val writerOptions = OrcFile.writerOptions(orcConfig)
      .setSchema(schemaProvider.ORCSchema)
      .memory(NoOpMemoryManager)
      .compress(CompressionKind.ZLIB)
      .bufferSize(OptimalCompressBuffer)
      .enforceBufferSize
      .fileSystem(new SimpleGCSFileSystem(gcs, stats))

    val orc = new OrcAppender(schemaProvider, delimiter, writerOptions, partSize, baseUri, stats)

    orc.append(lines)
  }
}
