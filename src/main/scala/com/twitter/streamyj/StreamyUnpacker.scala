package com.twitter.streamyj

import java.lang.reflect._
import scala.collection.mutable
import scala.reflect.Manifest
import org.codehaus.jackson.JsonProcessingException
import org.objenesis.ObjenesisStd

class JsonUnpackingException(reason: String) extends JsonProcessingException(reason) {
  def this(name: String, cls: Class[_], other: String) =
    this("Missing field conversion: " + name + " of type " + cls + " missing conversion from " + other)
}

class StreamyUnpacker {
  val objenesis = new ObjenesisStd()
  var ignoreExtraFields = false

  def coerce[A, B](name: String, obj: A, cls: Class[B])(implicit manifest: Manifest[A]): B = {
    if (manifest.erasure == classOf[Long]) {
      coerceLong(name, cls, obj.asInstanceOf[Long])
    } else if (manifest.erasure == classOf[Double]) {
      coerceDouble(name, cls, obj.asInstanceOf[Double])
    } else if (manifest.erasure == classOf[String]) {
      coerceString(name, cls, obj.asInstanceOf[String])
    } else {
      throw new JsonUnpackingException("foo")
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

  /*
  def methodsMatching(obj: AnyRef, name: String) = {
    obj.getClass.getMethods.find { method =>
      method.getName == name &&
        method.getReturnType == classOf[Unit] &&
        method.getParameterTypes.size == 1
    }.toList
  }
*/



  def setBooleanField[T](obj: T, field: Field, value: Boolean) {
    val t = field.getType
    if (t == classOf[Boolean]) {
      field.setBoolean(obj, value)
    } else {
      throw new JsonUnpackingException("Missing field conversion: " + field.getName + " of type " +
                                       field.getType.toString + " missing conversion from boolean")
    }
  }

  def setField[T](obj: T, field: Field, streamy: Streamy) {
    streamy.next() match {
      case ValueLong(x) => field.set(obj, coerce(field.getName, x, field.getType))
      case ValueDouble(x) => field.set(obj, coerce(field.getName, x, field.getType))
      case ValueString(x) => field.set(obj, coerce(field.getName, x, field.getType))
      case ValueFalse => setBooleanField(obj, field, false)
      case ValueTrue => setBooleanField(obj, field, true)
//      case StartArray => setArrayField(obj, field, getArray(streamy))
      case x =>
        throw new JsonUnpackingException("Unexpected token: " + x)

/*
      case object StartArray extends StreamyToken
      case object EndArray extends StreamyToken
      case object StartObject extends StreamyToken
      case object EndObject extends StreamyToken
      case object ValueNull extends StreamyToken
*/

      // null
      // array, object
    }
  }

  @throws(classOf[JsonProcessingException])
  def unpackObject[T](streamy: Streamy, cls: Class[T]): T = {
    val (obj, fields) = makeObject(cls)

    streamy.obj {
      case FieldName(name) =>
        fields.find { _.getName == name } match {
          case None =>
            if (!ignoreExtraFields) {
              throw new JsonUnpackingException("Extra field in json object for " + cls + ": " + name)
            }
          case Some(field) =>
            setField(obj, field, streamy)
        }
    }

    /*
    fields.foreach { field =>
      json.get(field.getName) match {
        case None =>
          throw new JsonException("Missing field: " + field.getName)
        case Some(value) =>
          setField(obj, field, value)
      }
    }

    if (!ignoreExtraFields) {
      val extraFields = json.keys -- fields.map { _.getName }
      if (extraFields.size > 0) {
        throw new JsonException("Extra fields in json: " + extraFields.mkString(", "))
      }
    }
    */

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
  def apply[T](s: String)(implicit manifest: Manifest[T]) =
    new StreamyUnpacker().unpackObject(new Streamy(s), manifest.erasure)
}
