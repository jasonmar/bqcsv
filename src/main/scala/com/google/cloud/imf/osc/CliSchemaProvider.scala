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

import com.google.cloud.bigquery.StandardSQLTypeName._
import com.google.cloud.bigquery.{Field, Schema, StandardSQLTypeName}
import com.google.cloud.imf.osc.Decoders.{DateDecoder, DecimalDecoder, Float64Decoder, Int64Decoder, StringDecoder, TimestampDecoder, TimestampDecoder2, TimestampDecoderZoned}

import scala.collection.immutable.ArraySeq

object CliSchemaProvider {
  def fields(schema: String): Seq[String] =
    ArraySeq.unsafeWrapArray(schema.split(','))

  def fieldArgs(field: String): SchemaField = {
    field.split(':') match {
      case Array(name) =>
        SchemaField(name, STRING)
      case Array(name,typ) =>
        SchemaField(name, StandardSQLTypeName.valueOf(typ))
      case Array(name,typ,args) =>
        val t = StandardSQLTypeName.valueOf(typ)
        t match {
          case TIMESTAMP|DATETIME|DATE =>
            if (args.contains('|')){
              // zone and format
              val Array(tz,format) = args.split('|')
              val timezone = Option(tz)
              SchemaField(name, t, timezone = timezone, format = format)
            } else if (args.contains("/")) {
              // zone only
              SchemaField(name, t, timezone = Option(args))
            } else {
              // format only
              val format = args
              SchemaField(name, t, format = format)
            }
          case NUMERIC =>
            val Array(p,s) = args.split('.')
            val precision = p.toInt
            val scale = s.toInt
            SchemaField(name, t, precision = precision, scale = scale)
          case STRING =>
            val length = args.toInt
            SchemaField(name, t, length = length)
          case x =>
            val msg = s"unknown args $args for $x field"
            throw new RuntimeException(msg)
        }

    }
  }

  case class SchemaField(name: String,
                         typ: StandardSQLTypeName,
                         length: Int = -1,
                         precision: Int = -1,
                         scale: Int = -1,
                         format: String = "",
                         timezone: Option[String] = None) {
    def decoder: Decoder = {
      typ match {
        case STRING =>
          StringDecoder(length)
        case DATE if format != "" =>
           DateDecoder(format)
        case DATE if format == "" =>
           DateDecoder()
        case TIMESTAMP|DATETIME if format.nonEmpty && timezone.isEmpty =>
          if (format.contains(Decoders.Offset)) TimestampDecoder(format)
          else TimestampDecoderZoned(format)
        case TIMESTAMP|DATETIME if format.isEmpty && timezone.isEmpty =>
          TimestampDecoder()
        case TIMESTAMP|DATETIME if timezone.isDefined && format.nonEmpty =>
          TimestampDecoder2(format, timezone.get)
        case TIMESTAMP|DATETIME if timezone.isDefined && format.isEmpty =>
          TimestampDecoder2(zoneId = timezone.get)
        case NUMERIC =>
          DecimalDecoder(precision,scale)
        case INT64 =>
          Int64Decoder()
        case FLOAT64 =>
          Float64Decoder()
        case t =>
          val msg = s"unsupported type $t"
          throw new RuntimeException(msg)
      }
    }
  }
}

case class CliSchemaProvider(schema: String) extends SchemaProvider {
  require(schema.nonEmpty, "schema not provided")
  import CliSchemaProvider._
  val fields: Seq[SchemaField] = ArraySeq.unsafeWrapArray(schema.split(',')).map(fieldArgs)
  override val fieldNames: Seq[String] = fields.map(_.name.toLowerCase)
  override val decoders: Array[Decoder] = fields.map(_.decoder).toArray

  override def bqSchema: Schema = {
    import scala.jdk.CollectionConverters.IterableHasAsJava
    val f = fieldNames.zip(decoders)
      .map{x => Field.newBuilder(x._1, x._2.bqType).build}.asJava
    Schema.of(f)
  }
}
