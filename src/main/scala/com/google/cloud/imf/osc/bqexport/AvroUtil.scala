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

import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.google.cloud.imf.osc.Decoders
import org.apache.avro.Schema

object AvroUtil {
  private val formatter = DateTimeFormatter.ofPattern(Decoders.ZoneFormat)
  private val dateFormatter = DateTimeFormatter.ofPattern(Decoders.DateFormat)

  def printTimestamp(t: java.sql.Timestamp): String =
    formatter.format(t.toInstant.atZone(Decoders.UTC))

  def printDate(t: LocalDate): String =
    dateFormatter.format(t)

  def getScale(schema: Schema): Int = schema.getJsonProp("scale").getIntValue

  def readDecimal(buf: ByteBuffer, bytes: Array[Byte], scale: Int): java.math.BigDecimal = {
    System.arraycopy(buf.array(),buf.position(),bytes,0,16)
    new java.math.BigDecimal(new java.math.BigInteger(bytes), scale)
  }

  def appendQuotedString(delimiter: Char, s: String, sb: StringBuilder): Unit = {
    if (s.contains(delimiter) || s.contains('\n')) {
      sb.append("\"")
      sb.append(s.replaceAllLiterally("\"","\\\"").replaceAllLiterally("\n","\\n"))
      sb.append("\"")
    } else sb.append(s)
  }
}
