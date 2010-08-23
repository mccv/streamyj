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

  def coerce[A, B](name: String, obj: A, cls: Class[B])(implicit manifest: Manifest[A]): B = {
    coerce(name, obj, manifest.erasure.asInstanceOf[Class[A]], cls)
  }

  def coerce[A, B](name: String, obj: A, objcls: Class[A], cls: Class[B]): B = {
    if (objcls == classOf[Long] || objcls == classOf[java.lang.Long]) {
      coerceLong(name, cls, obj.asInstanceOf[Long])
    } else if (objcls == classOf[Double]) {
      coerceDouble(name, cls, obj.asInstanceOf[Double])
    } else if (objcls == classOf[String]) {
      coerceString(name, cls, obj.asInstanceOf[String])
    } else if (objcls == classOf[Boolean]) {
      coerceBoolean(name, cls, obj.asInstanceOf[Boolean])
    } else if (objcls == classOf[List[AnyRef]]) {
      coerceList(name, cls, obj.asInstanceOf[List[AnyRef]])
    } else {
      throw new JsonUnpackingException("foo: " + objcls)
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

  def setField[T](obj: T, field: Field, streamy: Streamy) {
    streamy.next() match {
      case ValueLong(x) => field.set(obj, coerce(field.getName, x, field.getType))
      case ValueDouble(x) => field.set(obj, coerce(field.getName, x, field.getType))
      case ValueString(x) => field.set(obj, coerce(field.getName, x, field.getType))
      case ValueFalse => field.set(obj, coerce(field.getName, false, field.getType))
      case ValueTrue => field.set(obj, coerce(field.getName, true, field.getType))
      case StartArray => field.set(obj, coerce(field.getName, getArray(streamy), field.getType))
      case StartObject => field.set(obj, unpackObject(streamy, field.getType))
      case ValueNull => field.set(obj, null)
      case x =>
        throw new JsonUnpackingException("Unexpected token: " + x)
    }
  }

  def getArray(streamy: Streamy) = {
    val list = new mutable.ListBuffer[Any]
    getArrayNext(streamy, list).toList
  }

  def getArrayNext(streamy: Streamy, list: mutable.ListBuffer[Any]): mutable.ListBuffer[Any] = {
    streamy.next() match {
      case EndArray => return list
      case ValueLong(x) => list += x
      case ValueDouble(x) => list += x
      case ValueString(x) => list += x
      case ValueFalse => list += false
      case ValueTrue => list += true
      case ValueNull => list += null
      case StartArray => list += getArray(streamy)
//      case object StartObject extends StreamyToken
      case x =>
        throw new JsonUnpackingException("Unexpected token: " + x)
    }
    getArrayNext(streamy, list)
  }

  @throws(classOf[JsonProcessingException])
  def unpackObject[T](streamy: Streamy, cls: Class[T]): T = {
    val (obj, fields) = makeObject(cls)
    val seenFields = new mutable.ListBuffer[String]

    streamy.obj {
      case FieldName(name) =>
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
  def makeObject[T](cls: Class[T]): (T, List[Field]) = {
    val obj = objenesis.newInstance(cls).asInstanceOf[T]
    val fields = cls.getDeclaredFields().filter { field => !(field.getName contains '$') }.toList
    fields.foreach { _.setAccessible(true) }
    (obj, fields)
  }
}

object StreamyUnpacker {
  def apply[T](s: String)(implicit manifest: Manifest[T]): T =
    new StreamyUnpacker().unpackObject(new Streamy(s), manifest.erasure.asInstanceOf[Class[T]])
}
