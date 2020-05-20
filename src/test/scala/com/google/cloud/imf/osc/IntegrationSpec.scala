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

import com.google.cloud.imf.OSC
import org.scalatest.flatspec.AnyFlatSpec

class IntegrationSpec extends AnyFlatSpec {
  "BqCsv" should "upload" ignore {
    val args = Array(
      "--replace",
      "--autodetect",
      "--debug",
      "--dataset", "dataset",
      "--project", sys.env("PROJECT"),
      s"gs://${sys.env("BUCKET")}/sample1.orc",
      s"${sys.env("PROJECT")}:${sys.env("DATASET")}.${sys.env("TABLE")}",
      "src/test/resources/sample1.txt")
    OSCConfigParser.parse(args) match {
      case Some(cfg) =>
        Util.configureLogging(cfg.debug)
        OSC.run(cfg)
      case None =>
    }
  }
}
