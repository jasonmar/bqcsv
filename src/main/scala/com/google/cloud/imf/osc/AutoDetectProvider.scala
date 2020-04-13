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

import com.google.cloud.bigquery.{Field, Schema}

class AutoDetectProvider(override val fieldNames: Seq[String],
                         delimiter: Char,
                         sample: Array[String],
                         zoneId: String,
                         template: Option[Schema]) extends SchemaProvider {
  override val decoders: Array[Decoder] = {
    val rows = sample.map(_.split(delimiter))
    val cols = fieldNames.indices.map{i => rows.map(_.lift(i).getOrElse(""))}
    cols.map(SchemaInference.inferType(_,zoneId)).toArray
  }

  def print: String = {
    fieldNames.zip(decoders).map{x => s"${x._1} -> ${x._2}"}.mkString("\n")
  }

  override def bqSchema: Schema = {
    import scala.jdk.CollectionConverters.{IterableHasAsJava, IterableHasAsScala}
    val descriptions = template
      .map(_.getFields.asScala.map(x => (x.getName.toLowerCase,x.getDescription)).toMap)
      .getOrElse(Map.empty)
    val f = fieldNames.zip(decoders)
      .map{x =>
        val b = Field.newBuilder(x._1, x._2.bqType)
        descriptions.get(x._1.toLowerCase).foreach(b.setDescription)
        b.build
      }
      .asJava
    Schema.of(f)
  }
}

object AutoDetectProvider {
  private def colnames: String = "abcdefghijklmnopqrstuvwxyz0123456789"
  protected def colname(i: Int): String = {
    if (i < 36) new String(Array(colnames(i)))
    else {
      new String(Array(colnames(i%36),colnames(i/36)))
    }
  }

  def get(cfg: OSCConfig, sample: Array[String], schema: Option[Schema]): SchemaProvider = {
    import scala.jdk.CollectionConverters.IterableHasAsScala
    val fieldNames =
      schema
        .map(_.getFields.asScala.map(_.getName).toArray.toSeq)
        .getOrElse(
          if (cfg.schema.nonEmpty) cfg.schema.split(',').toSeq
          else sample.head.split(cfg.delimiter).indices.map(AutoDetectProvider.colname)
        )

    val sp = new AutoDetectProvider(fieldNames, cfg.delimiter, sample, cfg.zoneId, schema)
    System.out.println(s"inferred schema:\n${sp.print}")
    sp
  }
}