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
package com.google.cloud.imf.osc.bqexport


import com.google.cloud.bigquery.storage.v1.AvroRows
import com.google.cloud.imf.osc.{Logging, SimpleGCSFileSystem}
import com.google.cloud.storage.Storage
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.{CompressionCodec, DefaultCodec, SnappyCodec}
import org.apache.hadoop.io.{IOUtils, SequenceFile, Text}
import org.apache.hadoop.util.NativeCodeLoader

import scala.jdk.CollectionConverters.ListHasAsScala


class BQExportToSeqFile(schema: Schema,
                        id: Int,
                        gcs: Storage,
                        bucket: String,
                        name: String,
                        table: String)
  extends Logging with Export {
  private val stats = new FileSystem.Statistics(SimpleGCSFileSystem.Scheme)
  private val conf: Configuration = {
    val c = new Configuration()
    SequenceFile.setDefaultCompressionType(c, SequenceFile.CompressionType.BLOCK)
    c
  }

  private val fs: FileSystem = new SimpleGCSFileSystem(gcs, stats, conf)
  private val compressionCodec: CompressionCodec = {
    try {
      NativeCodeLoader.buildSupportsSnappy()
      val c = new SnappyCodec
      logger.info(s"Using Snappy compression")
      c.setConf(conf)
      c
    } catch {
      case _: UnsatisfiedLinkError =>
        val c = new DefaultCodec
        logger.warn(s"Snappy native libraries not available - using DefaultCodec")
        c.setConf(conf)
        c
    }
  }

  private val fields: IndexedSeq[AvroField] =
    schema.getFields.asScala.toArray.toIndexedSeq.map(AvroField)
  private val reader: GenericDatumReader[GenericRecord] =
    new GenericDatumReader[GenericRecord](schema)
  private var decoder: BinaryDecoder = _
  private var row: GenericRecord = _
  private var part: Int = 0

  // rows written across partitions
  private var rowCount: Long = 0
  private val LogFreq: Long = 1000000
  private var nextLog: Long = LogFreq

  // rows written to current partition
  private var partRowCount: Long = 0

  // 10M rows per partition
  private val PartRowLimit = 10L * 1000L * 1000L

  // buffer for current line
  private val sb: StringBuilder = new StringBuilder(128*1024)

  private var writer: SequenceFile.Writer = _
  private var objName: String = _
  private final val EmptyKey = new Text("")
  private final val KVClass = classOf[Text]

  def close(): Unit = {
    if (writer != null) {
      IOUtils.closeStream(writer)
      logger.info(s"Stream $id - $objName closed after writing $partRowCount rows")
      writer = null
      objName = null
    }
  }

  private def initWriter(): Unit = {
    if (partRowCount > PartRowLimit || writer == null){
      close()
      objName = s"gs://$bucket/$name/$table-$id-$part.seq"
      System.out.println(s"initWriter objName: $objName")
      logger.info(s"Stream $id - writing to $objName")
      writer = SequenceFile.createWriter(fs, fs.getConf, new Path(objName), KVClass, KVClass,
        SequenceFile.CompressionType.BLOCK, compressionCodec)
      part += 1
      partRowCount = 0
    }
  }

  def processRows(rows: AvroRows): Long = {
    decoder = DecoderFactory.get.binaryDecoder(rows.getSerializedBinaryRows.toByteArray, decoder)
    if (rowCount >= nextLog) {
      logger.info(s"Stream $id - $rowCount rows written")
      nextLog += LogFreq
    }
    initWriter()

    // rows written to current batch
    var batchRowCount: Long = 0
    while (!decoder.isEnd) {
      row = reader.read(row, decoder)
      sb.clear()
      var i = 0
      while (i < fields.length){
        if (i > 0)
          sb.append(',')
        val field = fields(i)
        field.read(row, sb)
        i += 1
      }
      sb.append('\n')
      writer.append(EmptyKey, new Text(sb.result()))

      rowCount += 1
      partRowCount += 1
      batchRowCount += 1
    }
    batchRowCount
  }
}



