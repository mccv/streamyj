package com.twitter.streamyj

import java.util.HashSet
import org.codehaus.jackson._
import org.specs._
import scala.collection.mutable.ListBuffer

object StreamySpec extends Specification {
  "Streamy" should {
    "handle scalar values" in {
      val s = Streamy("true false 123456789 3.1415927 \"hello world\"")
      s.readBoolean() must beTrue
      s.readBoolean() must beFalse
      s.readLong() mustEqual 123456789L
      s.readDouble() mustEqual 3.1415927
      s.readString() mustEqual "hello world"
    }

    "handle optional scalars" in {
      "null" in {
        Streamy("null").readNullOption() must beSome(null)
        Streamy("3").readNullOption() must beNone
      }
      "boolean" in {
        Streamy("null").readBooleanOption() must beNone
        Streamy("true").readBooleanOption() must beSome(true)
      }
      "long" in {
        Streamy("null").readLongOption() must beNone
        Streamy("42").readLongOption() must beSome(42L)
      }
      "double" in {
        Streamy("null").readDoubleOption() must beNone
        Streamy("3.14").readDoubleOption() must beSome(3.14)
      }
      "string" in {
        Streamy("null").readStringOption() must beNone
        Streamy("\"hello world\"").readStringOption() must beSome("hello world")
      }
    }

    "handle empty arrays" in {
      val s = Streamy("[] 42")
      s.readArray(_ => fail())
      s.readLong() mustEqual 42L
    }

    "handle simples arrays with readArray" in {
      val s = Streamy("[true, 123456789, 3.1415927, \"hello world\"] 42")
      var a0: Option[Boolean] = None
      var a1: Option[Long] = None
      var a2: Option[Double] = None
      var a3: Option[String] = None

      def verifyResults() {
        s.readLong() mustEqual 42L
        a0 must beSome(true)
        a1 must beSome(123456789L)
        a2 must beSome(3.1415927)
        a3 must beSome("hello world")
      }

      val fn: Int => Unit = {
        case 0 => a0 = Some(s.readBoolean())
        case 1 => a1 = Some(s.readLong())
        case 2 => a2 = Some(s.readDouble())
        case 3 => a3 = Some(s.readString())
      }

      "with readArray" in {
        s readArray fn
        verifyResults()
      }

      "with arr" in {
        s arr fn
        verifyResults()
      }

      "with readArrayOption" in {
        s readArrayOption fn must beTrue
        verifyResults()
      }

      "with startArray/readArrayBody" in {
        s.startArray
        s readArrayBody fn
        verifyResults()
      }
    }

    "read arrays piecemeal" in {
      val s = Streamy("[true, 123456789, 3.1415927, \"hello world\"] 42")
      s.startArray()
      s.readBoolean() must beTrue
      s.readLong() mustEqual 123456789L
      s.readDouble() mustEqual 3.1415927
      s.readString() mustEqual "hello world"
      s.skipToArrayEnd()
      s.readLong() mustEqual 42L
    }

    "handle nested arrays" in {
      val s = Streamy("[[[0, 1], [2, 3]]]")
      val seen = new ListBuffer[Long]
      def fn(index: Int) {
        s.next() match {
          case ValueLong(x) => seen += x
          case StartArray => s.readArrayBody(fn)
          case _ => throw new Exception
        }
      }
      s.readArray(fn)
      seen.toList mustEqual Range(0, 4).toList
    }

    "foldArray" in {
      val s = Streamy("[2, 4, 6, 8]")
      val res = s.foldArray(new ListBuffer[Int]) {
        case (buf, i) => buf += s.readInt(); buf
      }.toList
      res mustEqual List(2, 4, 6, 8)
    }

    "handle simple object, ignore unmatched fields" in {
      val s = Streamy("""{"id":1, "text":"text", "flag":true, "foo":3} 42""")
      var id: Option[Long] = None
      var text: Option[String] = None
      var flag: Option[Boolean] = None

      def verifyResults() {
        s.readLong() mustEqual 42L
        id must beSome(1)
        text must beSome("text")
        flag must beSome(true)
      }

      val fn: PartialFunction[String,Unit] = {
        case "id" => id = Some(s.readLong())
        case "text" => text = Some(s.readString())
        case "flag" => flag = Some(s.readBoolean())
      }

      "with \\ " in {
        s \ fn
        verifyResults()
      }

      "with readObject" in {
        s readObject fn
        verifyResults()
      }

      "with readObjectOption" in {
        s readObjectOption fn must beTrue
        verifyResults()
      }

      "with startObject/readObjectBody" in {
        s.startObject()
        s readObjectBody fn
        verifyResults()
      }
    }

    "foldObject" in {
      val s = Streamy("""{"id":1,"text":"hello"}""")
      val map = s.foldObject(Map[String,Any]()) {
        case (map, key) => map + (key -> s.readScalar())
      }
      map mustEqual Map("id" -> 1L, "text" -> "hello")
    }

    "can skip entire objects" in {
      val s = Streamy("""{"id":1, "text":"text", "bar":{"id":3, "bar":2}, "foo":2} 42""")
      // just make sure this doesn't throw anything (e.g. stack overflow)
      s.skipNext() must not(throwA[Exception])
      s.readLong() mustEqual 42L
    }

    "can skip entire arrays" in {
      val s = Streamy("""[42, true, {"id":1, "bar":{"id":3, "bar":2}}] 42""")
      // just make sure this doesn't throw anything (e.g. stack overflow)
      s.skipNext() must not(throwA[Exception])
      s.readLong() mustEqual 42L
    }

    "handle null objects correctly" in {
      val s = Streamy("""{"id":1, "text":"text", "bar":null, "foo":2}""")
      // just make sure this doesn't throw anything (e.g. stack overflow)
      var foo = 0L
      s \ {
        case "foo" => foo = s.readLong()
      }
      foo must be_==(2)
    }

    "handle embedded objects and arrays" in {
      val s = Streamy("""{"id":1, "embed":{"foo":"bar", "baz":{"baz":1}, "arr":[[1],2,3,4]},"id2":2}""")
      var id: Option[Long] = None
      var foo: Option[String] = None
      var baz: Option[Long] = None
      var id2: Option[Long] = None
      var arr: List[Any] = Nil
      def readArray(): List[Any] = {
        val buf = new ListBuffer[Any]
        s.readArray { _ =>
          s.peek() match {
            case _: ValueScalar => buf += s.readScalar()
            case StartArray => buf += readArray()
            case _ => s.skipNext()
          }
        }
        buf.toList
      }
      s \ {
        case "id" => id = Some(s.readLong())
        case "id2" => id2 = Some(s.readLong())
        case "embed" => s \ {
          case "foo" => foo = Some(s.readString())
          case "baz" => s \ {
            case "baz" => baz = Some(s.readLong())
          }
          case "arr" => arr = readArray()
        }
      }
      id must beSome(1L)
      id2 must beSome(2L)
      foo must beSome("bar")
      baz must beSome(1L)
      arr mustEqual List(List(1L), 2L, 3L, 4L)
    }
  }
}
