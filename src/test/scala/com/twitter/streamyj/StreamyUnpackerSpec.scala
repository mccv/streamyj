/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.streamyj

import org.specs._
import scala.collection.immutable


object StreamyUnpackerSpec extends Specification {
  "StreamyUnpacker" should {
    "object of ints" in {
      val data = """{"x":50,"y":25}"""

      case class Point(x: Int, y: Int)
      StreamyUnpacker[Point](data) mustEqual Point(50, 25)

      case class LPoint(x: Long, y: Int)
      StreamyUnpacker[LPoint](data) mustEqual LPoint(50L, 25)

      case class BPoint(x: Byte, y: Byte)
      StreamyUnpacker[BPoint](data) mustEqual BPoint(50.toByte, 25.toByte)

      case class FPoint(x: Float, y: Double)
      StreamyUnpacker[FPoint](data) mustEqual FPoint(50.0f, 25.0)

      case class SPoint(x: String, y: Int)
      StreamyUnpacker[SPoint](data) mustEqual SPoint("50", 25)

      case class BIPoint(x: BigInt, y: BigDecimal)
      StreamyUnpacker[BIPoint](data) mustEqual BIPoint(BigInt(50), BigDecimal(25))
    }

    "object of doubles" in {
      val data = """{"x":50.0,"y":25.0}"""

      case class Point(x: Int, y: Int)
      StreamyUnpacker[Point](data) mustEqual Point(50, 25)

      case class LPoint(x: Long, y: Int)
      StreamyUnpacker[LPoint](data) mustEqual LPoint(50L, 25)

      case class BPoint(x: Byte, y: Byte)
      StreamyUnpacker[BPoint](data) mustEqual BPoint(50.toByte, 25.toByte)

      case class FPoint(x: Float, y: Double)
      StreamyUnpacker[FPoint](data) mustEqual FPoint(50.0f, 25.0)

      case class SPoint(x: String, y: Int)
      StreamyUnpacker[SPoint](data) mustEqual SPoint("50.0", 25)

      case class BIPoint(x: BigInt, y: BigDecimal)
      StreamyUnpacker[BIPoint](data) mustEqual BIPoint(BigInt(50), BigDecimal(25))
    }

    "object of strings" in {
      val data = """{"x":"50","y":"25"}"""

      case class Point(x: Int, y: Int)
      StreamyUnpacker[Point](data) mustEqual Point(50, 25)

      case class LPoint(x: Long, y: Int)
      StreamyUnpacker[LPoint](data) mustEqual LPoint(50L, 25)

      case class BPoint(x: Byte, y: Byte)
      StreamyUnpacker[BPoint](data) mustEqual BPoint(50.toByte, 25.toByte)

      case class FPoint(x: Float, y: Double)
      StreamyUnpacker[FPoint](data) mustEqual FPoint(50.0f, 25.0)

      case class SPoint(x: String, y: Int)
      StreamyUnpacker[SPoint](data) mustEqual SPoint("50", 25)

      case class BIPoint(x: BigInt, y: BigDecimal)
      StreamyUnpacker[BIPoint](data) mustEqual BIPoint(BigInt(50), BigDecimal(25))
    }

  }
}
