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

package com.google.cloud.imf.bqcsv

import java.net.URI
import java.util.NoSuchElementException

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
 * @param batchSize size of ORC VectorizedRowBatch
 */
class OrcAppender(schemaProvider: SchemaProvider,
                  delimiter: Char,
                  writerOptions: WriterOptions,
                  partSize: Long,
                  baseUri: URI,
                  stats: FileSystem.Statistics,
                  batchSize: Int = 1024) extends Logging {
  private var partId: Int = 0
  private def partPath(): Path = {
    val bucket = baseUri.getAuthority
    val prefix = baseUri.getPath.stripPrefix("/")
    val path = new Path(s"gs://$bucket/$prefix/part-$partId.orc")
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

  /** Append records to partitioned ORC file
   *
   * @param lines Iterator containing lines of data
   * @return row count
   */
  def append(lines: Iterator[String]): Long = {
    var rowCount = 0L
    while (lines.hasNext){
      val path = partPath()
      val partWriter = OrcFile.createWriter(path, writerOptions)

      while (stats.getBytesWritten < partSize && lines.hasNext){
        rowCount += append(lines, partWriter)
      }
      stats.reset()
    }
    rowCount
  }

  /** Append records to ORC partition
   *
   * @param lines Iterator containing lines of data
   * @param partWriter ORC Writer for single output partition
   * @return row count
   */
  def append(lines: Iterator[String], partWriter: Writer): Long = {
    var rows: Long = 0
    try {
      while (lines.hasNext && stats.getBytesWritten < partSize) {
        rowBatch.reset()
        val batch = lines.take(batchSize)
        val rowId = OrcAppender.acceptBatch(batch, decoders, cols, delimiter)
        if (rowId == 0)
          rowBatch.endOfFile = true
        rowBatch.size = rowId
        rows += rowId
        partWriter.addRowBatch(rowBatch)
        partWriter match {
          case w: WriterImpl =>
            w.checkMemory(1.0d)
          case _ =>
        }
      }
    } finally {
      partWriter.close()
    }
    rows
  }
}

object OrcAppender {
  /**
   *
   * @param field String value of column
   * @param decoder Decoder instance
   * @param col ColumnVector to receive decoded value
   * @param rowId index within ColumnVector to store decoded value
   */
  @inline
  final def appendColumn(field: String, decoder: Decoder, col: ColumnVector, rowId: Int): Unit =
    decoder.get(field, col, rowId)

  /** Read
   * @param line single line of input
   * @param decoders Array[Decoder] to read from input
   * @param cols Array[ColumnVector] to receive Decoder output
   * @param rowId index within the batch
   */
  final def acceptRow(line: String,
                      decoders: Array[Decoder],
                      cols: Array[ColumnVector],
                      rowId: Int,
                      delimiter: Char): Unit = {
    val fields = line.split(delimiter)
    var i = 0
    while (i < decoders.length){
      try {
        appendColumn(fields(i), decoders(i), cols(i), rowId)
      } catch {
        case e: Exception =>
          System.err.println(s"failed on column $i\n${fields.lift(i)}")
          throw e
      }
      i += 1
    }
  }

  /** Accept a lines and append to batch
   *
   * @param lines lines of data for a single batch
   * @param decoders Decoder instances
   * @param cols VectorizedRowBatch
   * @return rowId
   */
  final def acceptBatch(lines: Iterator[String],
                        decoders: Array[Decoder],
                        cols: Array[ColumnVector],
                        delimiter: Char): Int = {
    var rowId = 0
    try {
      while (true) {
        val line = lines.next
        try {
          acceptRow(line, decoders, cols, rowId, delimiter)
        } catch {
          case e: Exception =>
            System.err.println(s"failed on row $rowId\n$line")
            throw e
        }
        rowId += 1
      }
    } catch {
      case _: NoSuchElementException =>
    }
    rowId
  }
}
