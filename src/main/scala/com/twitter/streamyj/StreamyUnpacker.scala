package com.twitter.streamyj

import java.lang.reflect._
import scala.collection.mutable
import scala.reflect.Manifest
import org.codehaus.jackson.JsonProcessingException
import org.objenesis.ObjenesisStd

class JsonUnpackingException(reason: String) extends JsonProcessingException(reason) {
  def this(name: String, cls: Class[_], other: String) =
    this("Missing field conversion: " + name + " of type (" + cls + ") missing conversion from " + other)
}

class StreamyUnpacker {
  val objenesis = new ObjenesisStd()
  var ignoreExtraFields = false
  var ignoreMissingFields = false

  private def coerce[A, B](name: String, obj: A, cls: Class[B])(implicit manifest: Manifest[A]): B = {
    coerce(name, obj, manifest.erasure.asInstanceOf[Class[A]], cls)
  }

  private def coerce[A, B](name: String, obj: A, objcls: Class[A], cls: Class[B]): B = {
    if (objcls == classOf[Long] || objcls == classOf[java.lang.Long]) {
      coerceLong(name, cls, obj.asInstanceOf[Long])
    } else if (objcls == classOf[Double]) {
      coerceDouble(name, cls, obj.asInstanceOf[Double])
    } else if (objcls == classOf[String]) {
      coerceString(name, cls, obj.asInstanceOf[String])
    } else if (objcls == classOf[Boolean]) {
      coerceBoolean(name, cls, obj.asInstanceOf[Boolean])
    } else if (objcls == classOf[List[AnyRef]] || objcls == classOf[::[AnyRef]]) {
      coerceList(name, cls, obj.asInstanceOf[List[AnyRef]])
    } else if (cls isAssignableFrom objcls) {
      obj.asInstanceOf[B]
    } else {
      throw new JsonUnpackingException("Don't know how to coerce " + objcls)
    }
  }

  private def coerceLong[T](name: String, cls: Class[T], value: Long): T = {
    (if (cls == classOf[Int]) {
      value.toInt
    } else if (cls == classOf[Long]) {
      value
    } else if (cls == classOf[Short]) {
      value.toShort
    } else if (cls == classOf[Char]) {
      value.toChar
    } else if (cls == classOf[Byte]) {
      value.toByte
    } else if (cls == classOf[Float]) {
      value.toFloat
    } else if (cls == classOf[Double]) {
      value.toDouble
    } else if (cls == classOf[String]) {
      value.toString
    } else if (cls == classOf[BigInt]) {
      BigInt(value)
    } else if (cls == classOf[BigDecimal]) {
      BigDecimal(value)
    } else {
      throw new JsonUnpackingException(name, cls, "long")
    }).asInstanceOf[T]
  }

  private def coerceDouble[T](name: String, cls: Class[T], value: Double): T = {
    (if (cls == classOf[Int]) {
      value.toInt
    } else if (cls == classOf[Long]) {
      value.toLong
    } else if (cls == classOf[Short]) {
      value.toShort
    } else if (cls == classOf[Char]) {
      value.toChar
    } else if (cls == classOf[Byte]) {
      value.toByte
    } else if (cls == classOf[Float]) {
      value.toFloat
    } else if (cls == classOf[Double]) {
      value
    } else if (cls == classOf[String]) {
      value.toString
    } else if (cls == classOf[BigInt]) {
      BigInt(value.toLong)
    } else if (cls == classOf[BigDecimal]) {
      BigDecimal(value)
    } else {
      throw new JsonUnpackingException(name, cls, "double")
    }).asInstanceOf[T]
  }

  private def coerceString[T](name: String, cls: Class[T], value: String): T = {
    val rv: Any = if (cls == classOf[Int]) {
      value.toInt
    } else if (cls == classOf[Long]) {
      value.toLong
    } else if (cls == classOf[Short]) {
      value.toShort
    } else if (cls == classOf[Char]) {
      value.toInt.toChar
    } else if (cls == classOf[Byte]) {
      value.toByte
    } else if (cls == classOf[Float]) {
      value.toFloat
    } else if (cls == classOf[Double]) {
      value.toDouble
    } else if (cls == classOf[String]) {
      value
    } else if (cls == classOf[BigInt]) {
      BigInt(value)
    } else if (cls == classOf[BigDecimal]) {
      BigDecimal(value)
    } else {
      throw new JsonUnpackingException(name, cls, "string")
    }
    rv.asInstanceOf[T]
  }

  private def coerceBoolean[T](name: String, cls: Class[T], value: Boolean): T = {
    val rv: Any = if (cls == classOf[Boolean]) {
      value
    } else {
      throw new JsonUnpackingException(name, cls, "boolean")
    }
    rv.asInstanceOf[T]
  }

  private def coerceList[T, A <: AnyRef](name: String, cls: Class[T], list: List[A]): T = {
    val rv: Any = if (cls == classOf[Seq[A]]) {
      list.toSeq
    } else if (cls == classOf[List[A]]) {
      list
    } else if (cls.isArray) {
      // eww.
      val coercedItems = list.map { item => coerce(name, item, item.getClass.asInstanceOf[Class[Any]], cls.getComponentType) }
      val array = Array.newInstance(cls.getComponentType, list.size)
      for (i <- 0 until list.size) {
        Array.set(array, i, coercedItems(i))
      }
      array
    } else {
      throw new JsonUnpackingException(name, cls, "list")
    }
    rv.asInstanceOf[T]
  }

  private def setField[T](obj: T, field: Field, streamy: Streamy) {
    streamy.next() match {
      case ValueLong(x) => field.set(obj, coerce(field.getName, x, field.getType))
      case ValueDouble(x) => field.set(obj, coerce(field.getName, x, field.getType))
      case ValueString(x) => field.set(obj, coerce(field.getName, x, field.getType))
      case ValueBoolean(x) => field.set(obj, coerce(field.getName, x, field.getType))
      case StartArray => field.set(obj, coerce(field.getName, getArray(streamy, field.getType), field.getType))
      case StartObject => field.set(obj, unpackObject(streamy, field.getType, true))
      case ValueNull => field.set(obj, null)
      case x =>
        throw new JsonUnpackingException("Unexpected token: " + x)
    }
  }

  private def getArray(streamy: Streamy, cls: Class[_]) = {
    val list = new mutable.ListBuffer[Any]
    getArrayNext(streamy, list, cls).toList
  }

  private def getArrayNext(streamy: Streamy, list: mutable.ListBuffer[Any], cls: Class[_]): mutable.ListBuffer[Any] = {
    streamy.next() match {
      case EndArray => return list
      case ValueLong(x) => list += x
      case ValueDouble(x) => list += x
      case ValueString(x) => list += x
      case ValueBoolean(x) => list += x
      case ValueNull => list += null
      case StartArray =>
        if (!cls.isArray) {
          throw new JsonUnpackingException("Can't unpack nested lists due to type erasure (try arrays)")
        }
        list += getArray(streamy, cls.getComponentType)
      case StartObject =>
        if (!cls.isArray) {
          throw new JsonUnpackingException("Can't unpack lists of objects due to type erasure (try arrays)")
        }
        list += unpackObject(streamy, cls.getComponentType, true)
      case x =>
        throw new JsonUnpackingException("Unexpected token: " + x)
    }
    getArrayNext(streamy, list, cls)
  }

  @throws(classOf[JsonProcessingException])
  def unpackObject[T](streamy: Streamy, cls: Class[T]): T = unpackObject(streamy, cls, false)

  @throws(classOf[JsonProcessingException])
  def unpackObject[T](streamy: Streamy, cls: Class[T], inObject: Boolean): T = {
    val (obj, fields) = makeObject(cls)
    val seenFields = new mutable.ListBuffer[String]

    if (!inObject) streamy.startObject()

    streamy.readObjectBody {
      case name =>
        fields.find { _.getName == name } match {
          case None =>
            if (!ignoreExtraFields) {
              throw new JsonUnpackingException("Extra field in json object for (" + cls + "): " + name)
            }
          case Some(field) =>
            setField(obj, field, streamy)
        }
        seenFields += name
    }

    if (! ignoreMissingFields) {
      val missingFields = fields.map { _.getName } -- seenFields.toList
      if (missingFields.size > 0) {
        throw new JsonUnpackingException("Missing field(s) in json object for (" + cls + "): " + missingFields.mkString(", "))
      }
    }

    obj
  }

  /**
   * So evil. Make an object without calling its constructor. Then find all
   * the declared fields and make them accessible. This opens up a case class
   * for naughtiness.
   */
  private def makeObject[T](cls: Class[T]): (T, List[Field]) = {
    val obj = objenesis.newInstance(cls).asInstanceOf[T]
    val fields = cls.getDeclaredFields().filter { field => !(field.getName contains '$') }.toList
    fields.foreach { _.setAccessible(true) }
    (obj, fields)
  }
}

object StreamyUnpacker {
  def apply[T](s: String)(implicit manifest: Manifest[T]): T = {
    new StreamyUnpacker().unpackObject(Streamy(s), manifest.erasure.asInstanceOf[Class[T]])
  }
}
