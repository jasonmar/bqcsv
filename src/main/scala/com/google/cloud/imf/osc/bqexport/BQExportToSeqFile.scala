package com.google.cloud.imf.osc.bqexport


import java.io.{BufferedOutputStream, IOException, OutputStreamWriter, Writer}
import java.nio.channels.Channels
import java.nio.file.{Files, Paths}

import com.google.cloud.bigquery.storage.v1.AvroRows
import com.google.cloud.imf.BQExport.logger
import com.google.cloud.imf.osc.Logging
import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageOptions}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Options.CreateOpts
import org.apache.hadoop.fs.{CreateFlag, FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.{CompressionType, Metadata}
import org.apache.hadoop.io.compress.{CompressionCodec, GzipCodec}
import org.apache.hadoop.io.{IOUtils, SequenceFile, Text}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.ListHasAsScala


class BQExportToSeqFile(schema: Schema,
                        id: Int,
                 gcs: Storage,
                 bucket: String,
                 name: String,
                 table: String) extends Logging with Export {
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
  private val BufSz = 1024*1024

  // rows written to current partition
  private var partRowCount: Long = 0

  // 10M rows per partition
  private val PartRowLimit = 10L * 1000L * 1000L

  // buffer for current line
  private val sb: StringBuilder = new StringBuilder(128*1024)

  private var writer: SequenceFile.Writer = _
  private var objName: String = _
  private var conf = new Configuration()
  private var seqFilesList = new ListBuffer[String]()
  def close(): Unit = {
    if (writer != null) {
      IOUtils.closeStream(writer);

      for (localSeqFile <- seqFilesList){
        uploadObject(gcs, bucket, localSeqFile, localSeqFile)
        deleteLocalFile(localSeqFile)
      }
      logger.info(s"Stream $id - gs://$bucket/$objName closed after writing " +
        s"$partRowCount rows")
      writer = null
      objName = null
    }
  }
  private def initWriter(): Unit = {
    if (partRowCount > PartRowLimit || writer == null){
      close()
      objName = s"$name/$table-$id-$part.seq"
      seqFilesList += objName
      println("initWriter objName: $objName")
      logger.info(s"Stream $id - writing to $objName")

      val outPath = new Path(objName);
      val fs = FileSystem.get(conf);
      val key = new Text();
      val value = new Text();

      writer = SequenceFile.createWriter(fs, conf, outPath, key.getClass(),value.getClass());

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
      writer.append(new Text(""), new Text(sb.result()));

      rowCount += 1
      partRowCount += 1
      batchRowCount += 1
    }
    batchRowCount
  }

  //Upload the Sequence files to GCS
  def uploadObject(storage: Storage, bucketName: String, objectName: String, localFilePath: String): Unit = {
    val blobId = BlobId.of(bucketName, objectName)
    val blobInfo = BlobInfo.newBuilder(blobId).build
    storage.create(blobInfo, Files.readAllBytes(Paths.get(localFilePath)))
    logger.info(s"File  $localFilePath uploaded to bucket $bucketName as $objectName")
  }

  def deleteLocalFile(filePath: String): Unit ={
    try {
      Files.deleteIfExists(Paths.get(filePath));
    }
    catch {
      case ex: IOException=>{
        logger.info(s"Not able to delete the file $filePath")
      }
    }
  }
}



