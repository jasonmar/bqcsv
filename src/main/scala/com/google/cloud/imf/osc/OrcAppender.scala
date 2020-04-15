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

package com.google.cloud.imf.osc

import java.net.URI

import com.google.cloud.imf.osc.OrcAppender.AppendResult
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.exec.vector.{ColumnVector, VectorizedRowBatch}
import org.apache.orc.OrcFile.WriterOptions
import org.apache.orc.impl.WriterImpl
import org.apache.orc.{OrcFile, Writer}

/** Decodes lines into ORC VectorizedRowBatch
 *
 * @param schemaProvider SchemaProvider provides field decoders
 * @param delimiter field delimiter
 * @param writerOptions WriterOptions
 * @param partSize partition size in bytes
 * @param baseUri gs://bucket/prefix
 * @param stats FileSystem.Statistics used to track partition size
 * @param id appender id
 * @param batchSize size of ORC VectorizedRowBatch
 */
class OrcAppender(schemaProvider: SchemaProvider,
                  delimiter: Char,
                  writerOptions: WriterOptions,
                  partSize: Long,
                  baseUri: URI,
                  stats: FileSystem.Statistics,
                  id: String,
                  batchSize: Int = 1024,
                  errorLimit: Long = 0) extends Logging {
  private var partId: Int = 0
  private var rowCount: Long = 0
  private var errorCount: Long = 0

  private def partPath(): Path = {
    val bucket = baseUri.getAuthority
    val prefix = baseUri.getPath.stripPrefix("/")
    val path = new Path(s"gs://$bucket/$prefix/part-$id-$partId.orc")
    partId += 1
    path
  }
  private final val decoders: Array[Decoder] = schemaProvider.decoders
  private final val cols: Array[ColumnVector] = decoders.map(_.columnVector(batchSize))
  private final val rowBatch: VectorizedRowBatch = {
    val batch = new VectorizedRowBatch(decoders.length, batchSize)
    for (i <- decoders.indices)
      batch.cols(i) = cols(i)
    batch
  }

  def newWriter(): Writer = {
    val path = partPath()
    logger.info(s"Created ORC Writer for $path")
    OrcFile.createWriter(path, writerOptions)
  }

  private var partWriter = newWriter()

  private def newPart(): Unit = {
    if (partWriter != null) {
      partWriter.close()
      logger.debug(s"Closed ORC Writer")
    }
    partWriter = newWriter()
    stats.reset()
  }

  /** Append records to partitioned ORC file
   *
   * @param lines Iterator containing lines of data
   * @return row count
   */
  def append(lines: Array[String]): AppendResult = {
    val result = append(lines, partWriter)
    rowCount += result.rowId
    logger.debug(s"$rowCount rows written")
    if (stats.getBytesWritten >= partSize)
      newPart()
    result
  }

  /** Append records to ORC partition
   *
   * @param batch Iterator containing lines of data
   * @param partWriter ORC Writer for single output partition
   * @return row count
   */
  private def append(batch: Array[String], partWriter: Writer): AppendResult = {
    var rows: Long = 0
    rowBatch.reset()
    val result = OrcAppender.acceptBatch(batch, decoders, cols, delimiter, errorLimit - errorCount)
    val rowId = result.rowId
    if (rowId == 0) {
      logger.debug(s"reached EOF - $rowId rows accepted in current batch")
      rowBatch.endOfFile = true
    }
    rowBatch.size = rowId
    rows += rowId
    partWriter.addRowBatch(rowBatch)
    logger.debug(s"added VectorizedRowBatch with size $rowId")
    partWriter match {
      case w: WriterImpl =>
        w.checkMemory(0)
      case _ =>
    }
    result
  }

  def close(): Unit = {
    if (partWriter != null) {
      logger.info(s"Closing ORC Writer")
      partWriter.close()
    }
  }
}

object OrcAppender extends Logging {
  /**
   *
   * @param field String value of column
   * @param decoder Decoder instance
   * @param col ColumnVector to receive decoded value
   * @param rowId index within ColumnVector to store decoded value
   */
  @inline
  private final def appendColumn(field: String, decoder: Decoder, col: ColumnVector, rowId: Int): Unit =
    decoder.get(field, col, rowId)

  /** Read
   * @param line single line of input
   * @param decoders Array[Decoder] to read from input
   * @param cols Array[ColumnVector] to receive Decoder output
   * @param rowId index within the batch
   */
  private final def acceptRow(line: String,
                              decoders: Array[Decoder],
                              cols: Array[ColumnVector],
                              rowId: Int,
                              delimiter: Char): Unit = {
    val fields = line.split(delimiter)
    var i = 0
    try {
      while (i < decoders.length){
        val s = if (i >= fields.length) "" else fields(i)
        appendColumn(s, decoders(i), cols(i), rowId)
        i += 1
      }
    } catch {
      case e: Exception =>
        val decoderType = decoders(i).getClass.getSimpleName.stripSuffix("$")
        val fieldValue = fields.lift(i).getOrElse("")
        val fieldLen = fields.lift(i).map(_.length).getOrElse(0)
        logger.error(s"Failed on column $i $decoderType ${decoders(i)} len=$fieldLen value='$fieldValue'")
        throw e
    }
  }

  /** Accept a lines and append to batch
   *
   * @param lines lines of data for a single batch
   * @param decoders Decoder instances
   * @param cols VectorizedRowBatch
   * @return rowId
   */
  private final def acceptBatch(lines: Array[String],
                                decoders: Array[Decoder],
                                cols: Array[ColumnVector],
                                delimiter: Char,
                                errorLimit: Long = 0): AppendResult = {
    var errorCount = 0L
    var rowId = 0
    while (rowId < lines.length) {
      val line = lines(rowId)
      try {
        acceptRow(line, decoders, cols, rowId, delimiter)
      } catch {
        case e: Exception =>
          errorCount += 1
          logger.error(s"Failed on row $rowId\n$line")
          if (errorCount >= errorLimit)
            throw e
      } finally {
        rowId += 1
      }
    }
    AppendResult(rowId, errorCount)
  }

  case class AppendResult(rowId: Int, errorCount: Long)
}
