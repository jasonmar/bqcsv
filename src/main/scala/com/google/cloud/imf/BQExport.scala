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

import java.util.concurrent.Executors

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.storage.v1.{BigQueryReadClient, CreateReadSessionRequest, DataFormat, ReadRowsRequest, ReadSession}
import com.google.cloud.imf.osc.bqexport.{BQExportToSeqFile, BQExporter, ExportConfig, ExportConfigParser}
import com.google.cloud.imf.osc.{GCS, Logging, Util}
import com.google.cloud.storage.Storage
import org.apache.avro.Schema

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object BQExport extends Logging {
  def main(args: Array[String]): Unit = {
    Util.configureLogging(true)
    ExportConfigParser.parse(args.toIndexedSeq, ExportConfig()) match {
      case Some(cfg) =>
        val client = BigQueryReadClient.create()
        val gcs = GCS.defaultClient(GoogleCredentials.getApplicationDefault())
        implicit val ec: ExecutionContext =
          ExecutionContext.fromExecutor(Executors.newWorkStealingPool(cfg.paralellism))
        run(cfg, client, gcs)
      case _ =>
        System.exit(1)
    }
  }

  def run(cfg: ExportConfig, client: BigQueryReadClient, gcs: Storage)
         (implicit ec: ExecutionContext): Unit = {
    val startTime = System.currentTimeMillis
    try {
      logger.info(s"Exporting ${cfg.projectId}:${cfg.dataset}.${cfg.table} to ${cfg.destUri}")
      if (cfg.filter.nonEmpty)
        logger.info(s"applying filter ${cfg.filter}")

      val session: ReadSession = client.createReadSession(
        CreateReadSessionRequest
          .newBuilder
          .setParent(cfg.projectPath)
          .setMaxStreamCount(cfg.paralellism)
          .setReadSession(
            ReadSession.newBuilder
              .setTable(cfg.tablePath)
              .setDataFormat(DataFormat.AVRO)
              .setReadOptions(cfg.readOpts)
              .build)
          .build())
      require(session.getStreamsCount > 0, "session must have at least 1 stream")

      val schema = new Schema.Parser().parse(session.getAvroSchema.getSchema)
      logger.debug(s"schema: ${schema.toString(false)}")

      val futures = (0 until session.getStreamsCount).map{streamId =>
        val readRowsRequest = ReadRowsRequest.newBuilder
          .setReadStream(session.getStreams(streamId).getName)
          .build

        Future {
          var rowCount: Long = 0
          val exporter = if (cfg.outputFileType == "SEQ") {
            new BQExportToSeqFile(schema, streamId, gcs, cfg.bucket, cfg.name, cfg.table)
          }else{
            new BQExporter(schema, streamId, gcs, cfg.bucket, cfg.name, cfg.table)
          }
          client.readRowsCallable.call(readRowsRequest).forEach{res =>
            if (res.hasAvroRows)
              rowCount += exporter.processRows(res.getAvroRows)
          }
          exporter.close()
          logger.info(s"Stream $streamId closed after receiving $rowCount rows")
          rowCount
        }
      }

      val results = Await.result(Future.sequence(futures), Duration.Inf)
      val totalRowCount = results.foldLeft(0L){_ + _}
      val endTime = System.currentTimeMillis
      val elapsedSeconds = (endTime - startTime) / 1000L
      logger.info(s"Finished - $totalRowCount total rows across ${session.getStreamsCount} " +
        s"streams in $elapsedSeconds seconds")
    } finally {
      client.close()
    }
  }
}
