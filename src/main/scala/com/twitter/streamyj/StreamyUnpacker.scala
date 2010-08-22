package com.twitter.streamyj

import java.lang.reflect._
import scala.reflect.Manifest
import org.codehaus.jackson.JsonProcessingException
import org.objenesis.ObjenesisStd

class JsonUnpackingException(reason: String) extends JsonProcessingException(reason)

class StreamyUnpacker {
  val objenesis = new ObjenesisStd()
  var ignoreExtraFields = false

  /*
  def methodsMatching(obj: AnyRef, name: String) = {
    obj.getClass.getMethods.find { method =>
      method.getName == name &&
        method.getReturnType == classOf[Unit] &&
        method.getParameterTypes.size == 1
    }.toList
  }
*/
  def setLongField[T](obj: T, field: Field, value: Long) {
    val t = field.getType
    if (t == classOf[Int]) {
      field.setInt(obj, value.toInt)
    } else if (t == classOf[Long]) {
      field.setLong(obj, value)
    } else if (t == classOf[Short]) {
      field.setShort(obj, value.toShort)
    } else if (t == classOf[Char]) {
      field.setChar(obj, value.toChar)
    } else if (t == classOf[Byte]) {
      field.setByte(obj, value.toByte)
    } else if (t == classOf[Float]) {
      field.setFloat(obj, value.toFloat)
    } else if (t == classOf[Double]) {
      field.setDouble(obj, value.toDouble)
    } else if (t == classOf[String]) {
      field.set(obj, value.toString)
    } else if (t == classOf[BigInt]) {
      field.set(obj, BigInt(value))
    } else if (t == classOf[BigDecimal]) {
      field.set(obj, BigDecimal(value))
    } else {
      throw new JsonUnpackingException("Missing field conversion: " + field.getName + " of type " +
                                       field.getType.toString + " missing conversion from long")
    }
  }

  def setDoubleField[T](obj: T, field: Field, value: Double) {
    val t = field.getType
    if (t == classOf[Int]) {
      field.setInt(obj, value.toInt)
    } else if (t == classOf[Long]) {
      field.setLong(obj, value.toLong)
    } else if (t == classOf[Short]) {
      field.setShort(obj, value.toShort)
    } else if (t == classOf[Char]) {
      field.setChar(obj, value.toChar)
    } else if (t == classOf[Byte]) {
      field.setByte(obj, value.toByte)
    } else if (t == classOf[Float]) {
      field.setFloat(obj, value.toFloat)
    } else if (t == classOf[Double]) {
      field.setDouble(obj, value)
    } else if (t == classOf[String]) {
      field.set(obj, value.toString)
    } else if (t == classOf[BigInt]) {
      field.set(obj, BigInt(value.toLong))
    } else if (t == classOf[BigDecimal]) {
      field.set(obj, BigDecimal(value))
    } else {
      throw new JsonUnpackingException("Missing field conversion: " + field.getName + " of type " +
                                       field.getType.toString + " missing conversion from double")
    }
  }

  def setStringField[T](obj: T, field: Field, value: String) {
    val t = field.getType
    if (t == classOf[Int]) {
      field.setInt(obj, value.toInt)
    } else if (t == classOf[Long]) {
      field.setLong(obj, value.toLong)
    } else if (t == classOf[Short]) {
      field.setShort(obj, value.toShort)
    } else if (t == classOf[Char]) {
      field.setChar(obj, value.toInt.toChar)
    } else if (t == classOf[Byte]) {
      field.setByte(obj, value.toByte)
    } else if (t == classOf[Float]) {
      field.setFloat(obj, value.toFloat)
    } else if (t == classOf[Double]) {
      field.setDouble(obj, value.toDouble)
    } else if (t == classOf[String]) {
      field.set(obj, value)
    } else if (t == classOf[BigInt]) {
      field.set(obj, BigInt(value))
    } else if (t == classOf[BigDecimal]) {
      field.set(obj, BigDecimal(value))
    } else {
      throw new JsonUnpackingException("Missing field conversion: " + field.getName + " of type " +
                                       field.getType.toString + " missing conversion from string")
    }
  }

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
      case ValueLong(x) => setLongField(obj, field, x)
      case ValueDouble(x) => setDoubleField(obj, field, x)
      case ValueString(x) => setStringField(obj, field, x)
      case ValueFalse => setBooleanField(obj, field, false)
      case ValueTrue => setBooleanField(obj, field, true)
      case x =>
        throw new JsonUnpackingException("Unexpected token: " + x)

/*
      case object StartArray extends StreamyToken
      case object EndArray extends StreamyToken
      case object StartObject extends StreamyToken
      case object EndObject extends StreamyToken
      case object ValueFalse extends StreamyToken
      case object ValueTrue extends StreamyToken
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
