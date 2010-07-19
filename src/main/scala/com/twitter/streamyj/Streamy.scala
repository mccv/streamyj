package com.twitter.streamyj

import org.codehaus.jackson._
import org.codehaus.jackson.JsonToken._
import scala.collection.mutable.HashMap


case class StreamyToken()
case object EndArray extends StreamyToken
case object StartArray extends StreamyToken
case object StartObject extends StreamyToken
case object EndObject extends StreamyToken
case class FieldName(name: String) extends StreamyToken
case object NotAvailable extends StreamyToken
case object ValueFalse extends StreamyToken
case object ValueTrue extends StreamyToken
case object ValueNull extends StreamyToken
case class ValueScalar(value: Any) extends StreamyToken
case class ValueLong(override val value: Long) extends ValueScalar(value.toString)
case class ValueDouble(override val value: Double) extends ValueScalar(value.toString)
case class ValueString(override val value: String) extends ValueScalar(value)


object Streamy {
  type ParseFunc = PartialFunction[StreamyToken, Unit]
}

class Streamy(s: String) {

  type ParseFunc = Streamy.ParseFunc
  val factory = new JsonFactory()
  val reader = factory.createJsonParser(s)

  var token:StreamyToken = null
  var fieldName:String = null
  var nestingLevel = 0

  def tokenToCaseClass(token: JsonToken) = token match {
    case START_ARRAY => StartArray
    case END_ARRAY => EndArray
    case START_OBJECT => StartObject
    case END_OBJECT => EndObject
    case FIELD_NAME => FieldName(reader.getCurrentName)
    case NOT_AVAILABLE => NotAvailable
    case VALUE_FALSE => ValueFalse
    case VALUE_TRUE => ValueTrue
    case VALUE_NULL => ValueNull
    case VALUE_NUMBER_FLOAT => ValueDouble(reader.getDoubleValue())
    case VALUE_NUMBER_INT => ValueLong(reader.getLongValue())
    case VALUE_STRING => ValueString(reader.getText())
    case _ => NotAvailable
  }

  val eat:ParseFunc = {
    case token if token != NotAvailable => //noop
  }

  def \(fn: ParseFunc) = {
    obj(fn)
  }

  def obj(fn: ParseFunc) = {
    startObject(fn)
  }

  def arr(fn: ParseFunc) = {
    startArray(fn)
  }

  def next():StreamyToken = {
    token = tokenToCaseClass(reader.nextToken())
    token
  }

  def startObject(fn: ParseFunc):Unit = {
    next() match {
      case token if fn.isDefinedAt(token) => {
        fn(token)
      }
      case ValueNull => return
      case EndObject => return
      case _ => //noop
    }
    readObject(fn)
  }

  def readObject(fn: ParseFunc):Unit = {
    next() match {
      case token if fn.isDefinedAt(token) => {
        fn(token)
      }
      case NotAvailable => return
      case EndObject => return
      case StartObject => readObject(eat)
      case _ => //noop
    }
    readObject(fn)
  }

  def startArray(fn: ParseFunc):Unit = {
    next() match {
      case token if fn.isDefinedAt(token) => {
        fn(token)
      }
      case ValueNull => return
      case EndArray => return
      case _ => //noop
    }
    readArray(fn)
  }

  def readArray(fn: ParseFunc):Unit = {
    next () match {
      case token if fn.isDefinedAt(token) => {
        fn(token)
      }
      case NotAvailable => return
      case EndArray => return
      case StartArray => readArray(eat)
      case _ => //noop
    }
    readArray(fn)
  }

  def readField() = {
    next() match {
      case ValueScalar(rv) => rv
      case _ => throw new IllegalArgumentException("tried to read a non-scalar field as a string")
    }
  }

  def readStringField() = {
    next() match {
      case ValueScalar(rv) => rv.toString
      case _ => throw new IllegalArgumentException("tried to read a non-scalar field as a string")
    }
  }

  def readLongField() = {
    next() match {
      case ValueLong(rv) => rv
      case _ => throw new IllegalArgumentException("tried to read a non-int field as a long")
    }
  }

  def readDoubleField() = {
    next() match {
      case ValueDouble(rv) => rv
      case _ => throw new IllegalArgumentException("tried to read a non-numeric field as a double")
    }
  }
}

  
