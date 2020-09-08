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

import java.nio.charset.StandardCharsets
import java.util.concurrent.{ArrayBlockingQueue, Callable, Executors, TimeUnit}

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.{BigQuery, ExternalTableDefinition, JobId, JobInfo,
  StandardTableDefinition, TableId}
import com.google.cloud.imf.osc.{AutoDetectProvider, BQ, CliSchemaProvider, GCS, Logging,
  NoOpMemoryManager, OSCConfig, OSCConfigParser, OrcAppender, SchemaProvider, SimpleGCSFileSystem,
  TableSchemaProvider, Util}
import com.google.cloud.storage.Storage
import com.google.common.collect.Queues
import com.google.common.io.Resources
import com.google.rpc.{Code, Status}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.orc.{CompressionKind, OrcConf, OrcFile}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.{Failure, Try}

object OSC extends Logging {
  def main(args: Array[String]): Unit = {
    System.out.println(Resources.toString(Resources.getResource("osc_build.txt"),StandardCharsets.UTF_8))
    OSCConfigParser.parse(args) match {
      case Some(cfg) =>
        Util.configureLogging(cfg.debug)
        val status = run(cfg)
        if (status.getCode != Code.OK_VALUE)
          System.exit(status.getCode)
      case _ =>
        System.exit(1)
    }
  }

  final val MegaByte: Long = 1024*1024
  final val OptimalCompressBuffer: Int = 32*1024

  def run(cfg: OSCConfig): Status = {
    val sources = cfg.source.map(f => Source.fromFile(f, "UTF-8"))
    val credentials = GoogleCredentials.getApplicationDefault.createScoped(Util.Scopes)
    val bq: BigQuery = BQ.defaultClient(cfg.projectId, cfg.location, credentials)
    val destTableId = BQ.resolveTableSpec(cfg.destTableSpec,cfg.projectId,cfg.datasetId)

    // Get Schema from BigQuery Table
    val templateTableId: TableId =
      if (cfg.templateTableSpec.nonEmpty) {
        logger.info(s"Getting schema from template table ${cfg.templateTableSpec}")
        BQ.resolveTableSpec(cfg.templateTableSpec,cfg.projectId,cfg.datasetId)
      } else {
        logger.info(s"Getting schema from destination table ${cfg.destTableSpec}")
        destTableId
      }
    logger.debug(s"getting table: $templateTableId")
    val table = Option(bq.getTable(templateTableId))
    logger.debug(s"table: $table")

    val schema = table.map(_.getDefinition[StandardTableDefinition].getSchema)
    logger.debug(s"schema: $schema")

    val lines = sources.foldLeft(Seq.empty[String].iterator){_ ++ _.getLines()}
    val sample = lines.take(cfg.sampleSize).toArray
    val gcs = GCS.defaultClient(credentials)
    val uri = new java.net.URI(cfg.stagingUri)

    // delete destination table if it is an external table
    val destTable = Option(bq.getTable(destTableId))
    if (cfg.replace){
      GCS.delete(gcs, uri)
      destTable match {
        case Some(tbl) if tbl.getDefinition.isInstanceOf[ExternalTableDefinition] =>
          logger.info(s"Deleting external table ${destTableId.getDataset}.${destTableId.getTable}")
          bq.delete(destTableId)
        case _ =>
      }
    } else {
      GCS.assertEmpty(gcs, uri)
      assert(destTable.isEmpty)
    }

    val sp: SchemaProvider =
      if (cfg.autodetect){
        AutoDetectProvider.get(cfg, sample, schema)
      } else if (cfg.templateTableSpec.nonEmpty || schema.nonEmpty) {
        if (schema.isEmpty)
          throw new RuntimeException(s"template table ${cfg.templateTableSpec} doesn't exist")
        TableSchemaProvider(schema.get, cfg.zoneId)
      } else {
        CliSchemaProvider(cfg.schema)
      }
    logger.info(s"schemaProvider:\n$sp")

    logger.debug(s"Creating thread pool with size ${cfg.parallelism}")
    implicit val ec: ExecutionContext = ExecutionContext
      .fromExecutorService(Executors.newWorkStealingPool(cfg.parallelism))

    logger.debug(s"Starting to write")
    val result = write(sample.iterator ++ lines, cfg.partSizeMB*MegaByte, cfg.delimiter, uri, sp,
      gcs, cfg.parallelism, cfg.errorLimit)

    val rowCount = result.foldLeft(0L){(a,b) => a + b.getOrElse(0L)}
    sources.foreach(_.close())
    logger.info(s"Wrote $rowCount rows")

    result.foreach{
      case Failure(e) =>
        logger.error(e.getMessage, e)
        throw e
      case _ =>
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
      val loadJobConfig = BQ.configureLoadJob(cfg, sp.bqSchema)
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
            gcs: Storage,
            parallelism: Int = 4,
            errorLimit: Long = 0)
           (implicit ec: ExecutionContext): IndexedSeq[Try[Long]] = {
    val orcConfig = {
      val c = new Configuration(false)
      OrcConf.COMPRESS.setString(c, "ZLIB")
      OrcConf.COMPRESSION_STRATEGY.setString(c, "SPEED")
      OrcConf.ENABLE_INDEXES.setBoolean(c, false)
      OrcConf.OVERWRITE_OUTPUT_FILE.setBoolean(c, true)
      OrcConf.MEMORY_POOL.setDouble(c, 0.5d)
      OrcConf.ROWS_BETWEEN_CHECKS.setLong(c, 1024*16)
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

    val batchSize = 1024
    val indices = 0 until parallelism
    val appenders = indices.map{i =>
      val id = i.toString
      new OrcAppender(schemaProvider, delimiter, writerOptions, partSize, baseUri, stats, id,
        batchSize, errorLimit)
    }
    val queues = indices.map(_ => Queues.newArrayBlockingQueue[Array[String]](64))
    val futures = indices.map{i => Future{
      new ORCAppendCallable(appenders(i), queues(i)).call()
    }}

    var i = 0
    var rows = 0L
    while (lines.hasNext){
      val batch = lines.take(batchSize).toArray
      queues(i).put(batch)
      rows += batch.length
      i += 1
      if (i >= parallelism)
        i = 0
    }
    queues.foreach(_.put(Array.empty))

    val results = Await.result(Future.sequence(futures), Duration(10, TimeUnit.MINUTES))

    val rowsWritten = results.foldLeft(0L)(_ + _.getOrElse(0L))
    logger.debug(s"$rowsWritten rows written")

    if (rowsWritten != rows)
      logger.error(s"rows read does not match rows written ($rows != $rowsWritten)")

    results
  }

  private class ORCAppendCallable(orc: OrcAppender,
                                  queue: ArrayBlockingQueue[Array[String]])
    extends Callable[Try[Long]] {
    override def call(): Try[Long] = Try{
      var rows: Long = 0
      val t = Thread.currentThread
      var continue = true
      while (!t.isInterrupted && continue){
        val batch = queue.take()
        if (batch.isEmpty)
          continue = false
        else {
          val result = orc.append(batch)
          rows += result.rowId
        }
      }
      orc.close()
      rows
    }
  }
}
