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

import com.google.cloud.bigquery.{Field, Schema, StandardTableDefinition, Table}
import com.google.cloud.imf.bqcsv.Decoders.{DateDecoder, DecimalDecoder, Float64Decoder, Int64Decoder, StringDecoder, TimestampDecoder, TimestampDecoder2}

import scala.jdk.CollectionConverters.IterableHasAsScala

case class TableSchemaProvider(schema: Schema) extends SchemaProvider {
  val fields: Array[Field] = schema.getFields.asScala.toArray
  override val fieldNames: Seq[String] = fields.map(_.getName.toLowerCase)
  override val decoders: Array[Decoder] = fields.map(TableSchemaProvider.decoder)
  override def bqSchema: Schema = schema
}

object TableSchemaProvider extends Logging {
  def apply(table: Table): TableSchemaProvider = {
    val schema = table.getDefinition[StandardTableDefinition].getSchema
    TableSchemaProvider(schema)
  }

  import com.google.cloud.bigquery.StandardSQLTypeName._
  def decoder(field: Field): Decoder = {
    (field.getType.getStandardType,Option(field.getDescription)) match {
      case (STRING,Some(len)) =>
        StringDecoder(len.toInt)
      case (STRING,_) =>
        StringDecoder()
      case (NUMERIC,Some(args)) =>
        val Array(precision,scale) = args.split(',')
        DecimalDecoder(precision.toInt,scale.toInt)
      case (DATE,Some(format)) =>
        DateDecoder(format)
      case (TIMESTAMP,Some(offset)) if offset.length < 8 =>
        TimestampDecoder2(offset = offset.toInt)
      case (TIMESTAMP,Some(args)) if args.contains('|') =>
        val Array(offset,format) = args.split('|')
        TimestampDecoder2(format, offset.toInt)
      case (DATETIME,Some(offset)) if offset.length < 8 =>
        TimestampDecoder2(offset = offset.toInt)
      case (DATETIME,Some(args)) if args.contains('|') =>
        val Array(offset,format) = args.split('|')
        TimestampDecoder2(format, offset.toInt)
      case (TIMESTAMP,Some(format)) =>
        TimestampDecoder(format)
      case (DATETIME,Some(format)) =>
        TimestampDecoder(format)
      case (DATE,_) =>
        DateDecoder()
      case (TIMESTAMP,_) =>
        TimestampDecoder()
      case (DATETIME,_) =>
        TimestampDecoder()
      case (INT64,_) =>
        Int64Decoder()
      case (FLOAT64,_) =>
        Float64Decoder()
    }
  }
}
