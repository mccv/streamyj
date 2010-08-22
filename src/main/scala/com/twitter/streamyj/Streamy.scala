package com.twitter.streamyj

import org.codehaus.jackson._
import org.codehaus.jackson.JsonToken._

/**
 * The base case class for a JsonToken
 */
abstract sealed case class StreamyToken()
/** maps to JsonToken.START_ARRAY */
case object StartArray extends StreamyToken
/** maps to JsonToken.END_ARRAY */
case object EndArray extends StreamyToken
/** maps to JsonToken.START_OBJECT */
case object StartObject extends StreamyToken
/** maps to JsonToken.END_OBJECT */
case object EndObject extends StreamyToken
/** maps to JsonToken.FIELD_NAME */
case class FieldName(name: String) extends StreamyToken
/** maps to JsonToken.NOT_AVAILABLE */
case object NotAvailable extends StreamyToken
/** maps to JsonToken.VALUE_FALSE */
case object ValueFalse extends StreamyToken
/** maps to JsonToken.VALUE_TRUE */
case object ValueTrue extends StreamyToken
/** maps to JsonToken.VALUE_NULL */
case object ValueNull extends StreamyToken
/** A base class for long, double, and string fields */
abstract case class ValueScalar(value: Any) extends StreamyToken
/** maps to JsonToken.VALUE_NUMBER_INT */
case class ValueLong(override val value: Long) extends ValueScalar(value)
/** maps to JsonToken.VALUE_NUMBER_FLOAT */
case class ValueDouble(override val value: Double) extends ValueScalar(value)
/** maps to JsonToken.VALUE_STRING */
case class ValueString(override val value: String) extends ValueScalar(value)


/**
 * Just store the PartialFunction type for parse functions so
 * it can be referenced by user implementations (easily)
 */
object Streamy {
  /**
   * This is the partial function parse methods need
   */
  type ParseFunc = PartialFunction[StreamyToken, Unit]
  /**
   * Jackson's Json parser factory
   */
  val factory = new JsonFactory()
  def createParser(s: String) = factory.createJsonParser(s)
}

/**
 * A helper for the Jackson JSON parser.
 *
 * Streamy allows you to write streaming parsers in an elegant,
 * Scala-idiomatic manner.  A quick example:
 *
 */
class Streamy(s: String) {

  /**
   * Pull in Streamy.ParseFunc just for brevity
   */
  type ParseFunc = Streamy.ParseFunc
  /**
   * the underlying json parser
   */
  val reader = Streamy.createParser(s)
  /**
   * Store the current token (it's useful while parsing)
   */
  var token:StreamyToken = null

  /**
   * A mapping of JsonToken constants to StreamyToken instances.
   * This allows (more elegant) pattern matching parsers
   */
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

  /**
   * A parse function that "eats" objects or arrays
   */
  val eat:ParseFunc = {
    case FieldName(s) => null; // noop
    case ValueFalse => null; // noop
    case ValueTrue => null; // noop
    case ValueScalar(s) => null; //noop
  }


  /**
   * alias for startObject(fn)
   */
  def \(fn: ParseFunc) = {
    startObject(fn)
  }

  /**
   * alias for startObject(fn)
   */
  def obj(fn: ParseFunc) = {
    startObject(fn)
  }

  /**
   * An alias for startArray.
   * Applies the supplied function to the current
   * JSON array.  Note that the current token should
   * be the start of the array (either an opening bracket or null)
   */
  def arr(fn: ParseFunc) = {
    startArray(fn)
  }

  /**
   * Advances the parser and sets the current token
   */
  def next():StreamyToken = {
    token = tokenToCaseClass(reader.nextToken())
    token
  }

  /**
   * applies the supplied function to the current
   * JSON object.  Note that the current token should
   * be the start of the object (either a curly brace or null).
   * After the first token is read this passes control to
   * readObject.
   */
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

  /**
   * Continues reading an object until it is closed.
   * If the passed in function is defined at the current token
   * it is called.  Otherwise the following actions are taken
   * <ul>
   * <li>NotAvailable: return. The JSON stream has ended.  Shouldn't happen</li>
   * <li>EndObject: return. The JSON object has ended.</li>
   * <li>StartObject: call readObject with the eat() handler.  Just consumes
   * tokens from the embedded object</li>
   * <li>StartArray: call readArray with the eat() handler.  Just consumes
   * tokens from the embedded array</li>
   * <li>Anything else: noop</li>
   */
  def readObject(fn: ParseFunc):Unit = {
    next() match {
      case token if fn.isDefinedAt(token) => {
        fn(token)
      }
      case NotAvailable => return
      case EndObject => return
      case StartArray => startArray(eat)
      case StartObject => startObject(eat)
      case _ => //noop
    }
    readObject(fn)
  }

  /**
   * applies the supplied function to the current
   * JSON array.  Note that the current token should
   * be the start of the array (either an opening bracket or null)
   */
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

  /*
   * Continues reading an array until it is closed.
   * If the passed in function is defined at the current token
   * it is called.  Otherwise the following actions are taken
   * <ul>
   * <li>NotAvailable: return. The JSON stream has ended.  Shouldn't happen</li>
   * <li>EndArray: return. The JSON array has ended.</li>
   * <li>StartObjoct: call readObject with the eat() handler.  Just consumes
   * tokens from the embedded object</li>
   * <li>StartArray: call readArray with the eat() handler.  Just consumes
   * tokens from the embedded array</li>
   * <li>Anything else: noop</li>
   */
  def readArray(fn: ParseFunc):Unit = {
    next () match {
      case token if fn.isDefinedAt(token) => {
        fn(token)
      }
      case NotAvailable => return
      case EndArray => return
      case StartArray => startArray(eat)
      case StartObject => startObject(eat)
      case _ => //noop
    }
    readArray(fn)
  }

  /**
   * reads a field of type Any
   */
  def readField() = {
    next() match {
      case ValueScalar(rv) => rv
      case _ => throw new IllegalArgumentException("tried to read a non-scalar field as a string")
    }
  }

  /**
   * reads a string field.  Throws an
   * IllegalArgumentException if the current value isn't a string
   */
  def readStringField() = {
    next() match {
      case ValueScalar(rv) => rv.toString
      case _ => throw new IllegalArgumentException("tried to read a non-scalar field as a string")
    }
  }

  /**
   * reads a long field.  Throws an
   * IllegalArgumentException if the current value isn't a long
   */
  def readLongField() = {
    next() match {
      case ValueLong(rv) => rv
      case _ => throw new IllegalArgumentException("tried to read a non-int field as a long")
    }
  }

  /**
   * reads a double field.  Throws an
   * IllegalArgumentException if the current value isn't a double or a long
   */
  def readDoubleField(): Double = {
    next() match {
      case ValueDouble(rv) => rv
      case ValueLong(rv) => rv
      case _ => throw new IllegalArgumentException("tried to read a non-numeric field as a double")
    }
  }
}
