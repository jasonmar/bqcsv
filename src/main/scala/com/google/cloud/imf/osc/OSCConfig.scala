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

case class OSCConfig(source: Seq[String] = Nil,
                     delimiter: Char = 'þ',
                     stagingUri: String = "",
                     destTableSpec: String = "",
                     templateTableSpec: String = "",
                     autodetect: Boolean = false,
                     replace: Boolean = false,
                     append: Boolean = false,
                     external: Boolean = false,
                     partSizeMB: Int = 128,
                     parallelism: Int = 1,
                     errorLimit: Int = 0,
                     zoneId: String = "America/Los_Angeles",
                     lifetime: Long = 7L*24L*60L*60L*1000L,
                     projectId: String = "",
                     datasetId: String = "",
                     location: String = "US",
                     schema: String = "",
                     sampleSize: Int = 4096,
                     debug: Boolean = false)
