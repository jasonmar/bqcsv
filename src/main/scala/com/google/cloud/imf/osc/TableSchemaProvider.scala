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

import com.google.cloud.bigquery.{Field, Schema, StandardTableDefinition, Table}
import com.google.cloud.imf.osc.Decoders.{DateDecoder, DecimalDecoder, Float64Decoder, Int64Decoder, StringDecoder, TimestampDecoder, TimestampDecoder2, TimestampDecoderZoned}

import scala.jdk.CollectionConverters.IterableHasAsScala

case class TableSchemaProvider(schema: Schema, zoneId: String) extends SchemaProvider {
  val fields: Seq[Field] = schema.getFields.asScala.toIndexedSeq
  override val fieldNames: Seq[String] = fields.map(_.getName.toLowerCase)
  override val decoders: Array[Decoder] =
    fields.map(TableSchemaProvider.decoder(_, zoneId)).toArray
  override def bqSchema: Schema = schema

  override def toString: String =
    s"""TableSchemaProvider
       |${decoders.zipWithIndex.zip(fieldNames).map(_.toString).mkString("\t","\n\t","\n")}
       |""".stripMargin
}

object TableSchemaProvider extends Logging {
  def apply(table: Table, zoneId: String): TableSchemaProvider = {
    val schema = table.getDefinition[StandardTableDefinition].getSchema
    TableSchemaProvider(schema, zoneId)
  }

  import com.google.cloud.bigquery.StandardSQLTypeName._
  def decoder(field: Field, zoneId: String): Decoder = {
    logger.debug(s"${field.getName} ${field.getType.getStandardType} ${field.getDescription} ")
    (field.getType.getStandardType,Option(field.getDescription)) match {
      case (STRING,Some(len)) =>
        StringDecoder(len.toInt)
      case (STRING,_) =>
        StringDecoder()
      case (NUMERIC,None) =>
//        logger.warn("NUMERIC precision and scale not set - using default of 18,5")
        DecimalDecoder(18,5)
      case (NUMERIC,Some(args)) =>
        val Array(precision,scale) = args.split(',')
        DecimalDecoder(precision.toInt,scale.toInt)
      case (DATE,Some(format)) =>
        DateDecoder(format)
      case (TIMESTAMP,Some(args)) if args.contains('|') =>
        val Array(zoneId,format) = args.split('|')
        TimestampDecoder2(format, zoneId)
      case (TIMESTAMP,Some(zoneId)) if zoneId.contains('/') =>
        TimestampDecoder2(zoneId = zoneId)
      case (TIMESTAMP,Some(format)) =>
        if (format.contains(Decoders.Offset)) TimestampDecoder(format)
        else TimestampDecoderZoned(format)
      case (DATE,_) =>
        DateDecoder()
      case (TIMESTAMP,_) =>
        TimestampDecoder()
      case (INT64,_) =>
        Int64Decoder()
      case (FLOAT64,_) =>
        Float64Decoder()
      case (t,_) =>
        throw new UnsupportedOperationException(s"Loading of $t columns from ORC is not supported")
    }
  }
}
