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

import com.google.cloud.bigquery.Schema
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

trait SchemaProvider {
  def fieldNames: Seq[String]

  def decoders: Array[Decoder]

  def ORCSchema: TypeDescription =
    fieldNames.zip(decoders)
      .foldLeft(new TypeDescription(Category.STRUCT)){(a,b) =>
          a.addField(b._1,b._2.typeDescription)
      }

  def bqSchema: Schema

  override def toString: String = ORCSchema.toJson
}
