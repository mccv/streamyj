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

    "object of booleans" in {
      case class CryForMe(cry: Boolean)
      StreamyUnpacker[CryForMe]("""{"cry":false}""") mustEqual CryForMe(false)
      StreamyUnpacker[CryForMe]("""{"cry":true}""") mustEqual CryForMe(true)
    }

    "object of arrays" in {
      val data = """{"name":"Santa","grades":[95, 79, 88, 90]}"""

      case class SeqGrades(name: String, grades: Seq[Int])
      StreamyUnpacker[SeqGrades](data) mustEqual SeqGrades("Santa", List(95, 79, 88, 90).toSeq)

      case class ListGrades(name: String, grades: List[Int])
      StreamyUnpacker[ListGrades](data) mustEqual ListGrades("Santa", List(95, 79, 88, 90))

      case class ArrayGrades(name: String, grades: Array[Int])
      StreamyUnpacker[ArrayGrades](data).grades.toList mustEqual List(95, 79, 88, 90)

      val data2 = """{"name":"Santa","friends":["Rudolph","Frosty"]}"""
      case class ArrayFriends(name: String, friends: Array[String])
      StreamyUnpacker[ArrayFriends](data2).friends.toList mustEqual List("Rudolph", "Frosty")
    }

    "nested objects" in {
      val data = """{"name":"Santa","employment":{"company":"Santa's Workshop","years":109}}"""
      case class Employment(company: String, years: Int)
      case class Employee(name: String, employment: Employment)
      StreamyUnpacker[Employee](data) mustEqual Employee("Santa", Employment("Santa's Workshop", 109))
    }

    "netsted arrays" in {
      val data = """{"name":"Vega","position":[[14,20],[15,34]]}"""

      case class ArrayStar(name: String, position: Array[Array[Int]])
      val star = StreamyUnpacker[ArrayStar](data)
      star.name mustEqual "Vega"
      star.position.map { _.toList }.toList mustEqual List(List(14, 20), List(15, 34))

      case class ListStar(name: String, position: List[List[Int]])
      StreamyUnpacker[ListStar](data) must throwA[Exception]
    }

    "array of objects" in {
      val data = """{"name":"Santa","roster":[{"name":"Robey","good":true},{"name":"Rus","good":true}]}"""

      case class Recipient(name: String, good: Boolean)
      case class ArrayGifter(name: String, roster: Array[Recipient])
      val arraySanta = StreamyUnpacker[ArrayGifter](data)
      arraySanta.name mustEqual "Santa"
      arraySanta.roster.toList mustEqual List(Recipient("Robey", true), Recipient("Rus", true))

      case class ListGifter(name: String, roster: List[Recipient])
      StreamyUnpacker[ListGifter](data) must throwA[Exception]
    }
  }
}
