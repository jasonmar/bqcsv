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

import scala.annotation.switch

object StringToNum {
  def decimalValue(s: String, targetScale: Int): Long = {
    val n = s.length
    val negative = s.charAt(0) == '-'
    var scale = -1
    var i = 0

    // skip to first digit
    if (negative) i += 1

    var x: Long = (s.charAt(i): @switch) match {
      case '0'  => 0
      case '1'  => 1
      case '2'  => 2
      case '3'  => 3
      case '4'  => 4
      case '5'  => 5
      case '6'  => 6
      case '7'  => 7
      case '8'  => 8
      case '9'  => 9
      case '.'  =>
        scale = 0
        0
      case _  => throw new RuntimeException("non-digit character in decimal field")
    }
    i += 1
    while (i < n){
      (s.charAt(i): @switch) match {
        case '0'  =>
          x *= 10
          x += 0
          if (scale > -1) scale += 1
        case '1'  =>
          x *= 10
          x += 1
          if (scale > -1) scale += 1
        case '2'  =>
          x *= 10
          x += 2
          if (scale > -1) scale += 1
        case '3'  =>
          x *= 10
          x += 3
          if (scale > -1) scale += 1
        case '4'  =>
          x *= 10
          x += 4
          if (scale > -1) scale += 1
        case '5'  =>
          x *= 10
          x += 5
          if (scale > -1) scale += 1
        case '6'  =>
          x *= 10
          x += 6
          if (scale > -1) scale += 1
        case '7'  =>
          x *= 10
          x += 7
          if (scale > -1) scale += 1
        case '8'  =>
          x *= 10
          x += 8
          if (scale > -1) scale += 1
        case '9'  =>
          x *= 10
          x += 9
          if (scale > -1) scale += 1
        case '.'  =>
          if (scale == -1) scale = 0
          else throw new RuntimeException("unexpected '.' character in decimal field")
        case _  => throw new RuntimeException("non-digit character in decimal field")
      }
      i += 1
    }

    // no decimal point
    if (scale == -1)
      scale = 0

    while (scale < targetScale){
      x *= 10
      scale += 1
    }

    while (scale > targetScale) {
      x /= 10
      scale -= 1
    }

    if (negative) x *= -1
    x
  }

  def longValue(s: String): Long = {
    val n = s.length
    val negative = s.charAt(0) == '-'
    var i = 0
    // skip to first digit
    if (negative) i = 1

    var x: Long = (s.charAt(i): @switch) match {
      case '0'  => 0
      case '1'  => 1
      case '2'  => 2
      case '3'  => 3
      case '4'  => 4
      case '5'  => 5
      case '6'  => 6
      case '7'  => 7
      case '8'  => 8
      case '9'  => 9
      case _  => throw new RuntimeException("non-digit character in INT64 field")
    }
    i += 1
    while (i < n){
      (s.charAt(i): @switch) match {
        case '0'  =>
          x *= 10
          x += 0
        case '1'  =>
          x *= 10
          x += 1
        case '2'  =>
          x *= 10
          x += 2
        case '3'  =>
          x *= 10
          x += 3
        case '4'  =>
          x *= 10
          x += 4
        case '5'  =>
          x *= 10
          x += 5
        case '6'  =>
          x *= 10
          x += 6
        case '7'  =>
          x *= 10
          x += 7
        case '8'  =>
          x *= 10
          x += 8
        case '9'  =>
          x *= 10
          x += 9
        case _  =>
          throw new RuntimeException("non-digit character in INT64 field")
      }
      i += 1
    }

    if (negative) x *= -1
    x
  }
}
