package com.twitter.streamyj

import java.io.{File, Reader, StringWriter}
import org.codehaus.jackson._
import org.codehaus.jackson.JsonToken._

/**
 * Just store the PartialFunction type for parse functions so
 * it can be referenced by user implementations (easily)
 */
object Streamy {
  type ObjectParseFunc = PartialFunction[String, Unit]
  type ArrayParseFunc = Function[Int, Unit]

  /**
   * Jackson's Json parser factory
   */
  val factory = new JsonFactory()

  def apply(source: String): Streamy = apply(factory.createJsonParser(source))
  def apply(reader: Reader): Streamy = apply(factory.createJsonParser(reader))
  def apply(file: File): Streamy = apply(factory.createJsonParser(file))
  def apply(parser: JsonParser): Streamy = new Streamy(parser)
}

/**
 * A helper for the Jackson JSON parser.
 *
 * Streamy allows you to write streaming parsers in an elegant,
 * Scala-idiomatic manner.  A quick example:
 *
 */
class Streamy(parser: JsonParser) {
  import Streamy.ObjectParseFunc
  import Streamy.ArrayParseFunc

  private var currentToken: StreamyToken = NotAvailable
  private var peekedToken: StreamyToken = NotAvailable

  /**
   * Advances the parser and sets the current token.
   * @throws JsonParseException if the are no more tokens
   */
  def next(): StreamyToken = {
    peekedToken match {
      case NotAvailable =>
        currentToken = StreamToken(parser)
      case token =>
        currentToken = token
        peekedToken = NotAvailable
    }
    currentToken
  }

  /**
   * Gets the current token, which is the last token returned by next()
   */
  def current = currentToken

  /**
   * Looks at the next token without consuming it.
   */
  def peek(): StreamyToken = {
    if (peekedToken == NotAvailable)
      peekedToken = StreamToken(parser)
    peekedToken
  }

//  def read[T](fn: PartialFunction[Token,T]): Option[T] = {}

  /**
   * Gets the location of the current token
   */
  def location = parser.getCurrentLocation

  /**
   * alias for readObject(fn)
   */
  def \(fn: ObjectParseFunc) = readObject(fn)

  /**
   * An alias for readArray.
   */
  def arr(fn: ArrayParseFunc) = readArray(fn)

  /**
   * Matches the start of an object value, but without reading the object
   * body, or throws an exception if the next token is not an StartObject.
   */
  def startObject() {
    next() match {
      case StartObject => // goo
      case token => unexpected(token, "object")
    }
  }

  /**
   * Matches the start of an object value, but without reading the object
   * body, or "null", or throws an exception if the next token is not an StartObject.
   * @return true if starting an object, false if null
   */
  def startObjectOption(): Boolean = {
    next() match {
      case ValueNull => false
      case StartObject => true
      case token => unexpected(token, "object")
    }
  }

  /**
   * Reads an object from open-curly to close-curly. Any field name not
   * recognized by the given PartialFunction will be skipped.
   * If the PartialFunction matches a field name, it MUST either fully
   * read the corresponding value or skip it.  Not doing so will leave
   * the parser in an unpredictable state.
   */
  def readObject(fn: ObjectParseFunc) {
    startObject()
    readObjectBody(fn)
  }

  /**
   * Reads an object from open-curly to close-curly. Any field name not
   * recognized by the given PartialFunction will be skipped.
   * If the PartialFunction matches a field name, it MUST either fully
   * read the corresponding value or skip it.  Not doing so will leave
   * the parser in an unpredictable state.
   */
  def readObjectOption(fn: ObjectParseFunc): Boolean = {
    startObjectOption() && { readObjectBody(fn); true }
  }

  /**
   * Reads the body of the object, up to the close-curly.  Any field name not
   * recognized by the given PartialFunction will be skipped.
   * The result of calling this is undefined if not already in an object.
   * If the PartialFunction matches a field name, it MUST either fully
   * read the corresponding value or skip it.  Not doing so will leave
   * the parser in an unpredictable state.
   */
  def readObjectBody(fn: ObjectParseFunc) {
    def loop() {
      next() match {
        case EndObject => // done
        case FieldName(name) =>
          if (fn.isDefinedAt(name)) fn(name) else skipNext()
          loop()
        case token => unexpected(token, "field name")
      }
    }
    loop()
  }

  /**
   * Reads an object using an accumulator.
   * If the PartialFunction matches a field name, it MUST either fully
   * read the corresponding value or skip it.  Not doing so will leave
   * the parser in an unpredictable state.
   */
  def foldObject[T](start: T)(fn: PartialFunction[(T,String), T]): T = {
    startObject()
    foldObjectBody(start)(fn)
  }

  /**
   * Reads an object using an accumulator.
   * If the PartialFunction matches a field name, it MUST either fully
   * read the corresponding value or skip it.  Not doing so will leave
   * the parser in an unpredictable state.
   */
  def foldObjectBody[T](start: T)(fn: PartialFunction[(T,String), T]): T = {
    def loop(accum: T): T = {
      next() match {
        case EndObject => accum
        case FieldName(name) =>
          val tup = (accum, name)
          if (fn.isDefinedAt(tup))
            loop(fn(tup))
          else {
            skipNext()
            loop(accum)
          }
        case token => unexpected(token, "field name")
      }
    }
    loop(start)
  }

  /**
   * Matches the start of an array value, but without reading the array
   * members, or throws an exception if the next token is not a StartArray.
   */
  def startArray() {
    next() match {
      case StartArray => // good
      case token => unexpected(token, "array")
    }
  }

  /**
   * Matches the start of an array value, but without reading the array
   * members, or "null", or throws an exception if the next token is not a StartArray.
   * @return true if starting an array, false if null.
   */
  def startArrayOption(): Boolean = {
    next() match {
      case ValueNull => false
      case StartArray => true
      case token => unexpected(token, "array")
    }
  }

  /**
   * applies the supplied function to the current
   * JSON array.  Note that the current token should
   * be the start of the array (either an opening bracket or null)
   * The given function MUST either fully read the corresponding value
   * or skip it.  Not doing so will leave the parser in an unpredictable state.
   */
  def readArray(fn: ArrayParseFunc) {
    startArray()
    readArrayBody(fn)
  }

  /**
   * applies the supplied function to the current
   * JSON array.  Note that the current token should
   * be the start of the array (either an opening bracket or null)
   * The given function MUST either fully read the corresponding value
   * or skip it.  Not doing so will leave the parser in an unpredictable state.
   */
  def readArrayOption(fn: ArrayParseFunc): Boolean = {
    startArrayOption() && { readArrayBody(fn); true }
  }

  /**
   * Reads the body of an array upto the close-bracket.
   * The given function MUST either fully read the corresponding value
   * or skip it.  Not doing so will leave the parser in an unpredictable state.
   */
  def readArrayBody(fn: ArrayParseFunc) {
    def loop(index: Int) {
      if (peek() == EndArray) {
        next() // skip ]
      } else {
        fn(index)
        loop(index + 1)
      }
    }
    loop(0)
  }

  /**
   * Reads an array using an accumulator.
   * The given function MUST either fully read the corresponding value
   * or skip it.  Not doing so will leave the parser in an unpredictable state.
   */
  def foldArray[T](start: T)(fn: (T,Int) => T): T = {
    startArray()
    foldArrayBody(start)(fn)
  }

  /**
   * Reads an array using an accumulator.
   * The given function MUST either fully read the corresponding value
   * or skip it.  Not doing so will leave the parser in an unpredictable state.
   */
  def foldArrayBody[T](start: T)(fn: (T,Int) => T): T = {
    def loop(accum: T, index: Int): T = {
      if (peek() == EndArray) {
        next() // skip ]
        accum
      } else {
        loop(fn(accum, index), index + 1)
      }
    }
    loop(start, 0)
  }

  /**
   * If the next value is "null", it is read and returned.  Otherwise,
   * nothing happens.
   */
  def readNullOption(): Option[Null] = peek() match {
    case ValueNull => next(); Some(null)
    case _ => None
  }

  /**
   * reads a field of type Any
   */
  def readScalar(): Any = next() match {
    case scalar: ValueScalar => scalar.value
    case token => unexpected(token, "scalar")
  }

  /**
   * reads a string value.  Throws a
   * JsonParseException if the current value isn't a string
   */
  def readString(): String = next() match {
    case scalar: ValueScalar => scalar.value.toString
    case token => unexpected(token, "string (or any scalar)")
  }

  /**
   * reads a string value or null.  Throws a
   * JsonParseException if the current value isn't a string
   */
  def readStringOption(): Option[String] = next() match {
    case ValueNull => None
    case scalar: ValueScalar => Some(scalar.value.toString)
    case token => unexpected(token, "string (or any scalar)")
  }

  /**
   * reads a boolean value.  throws a JsonParseException if
   * the current value isn't a boolean.
   */
  def readBoolean(): Boolean = next() match {
    case ValueBoolean(rv) => rv
    case token => unexpected(token, "boolean")
  }

  /**
   * reads a boolean value or null.  throws a JsonParseException if
   * the current value isn't a boolean.
   */
  def readBooleanOption(): Option[Boolean] = next() match {
    case ValueNull => None
    case ValueBoolean(rv) => Some(rv)
    case token => unexpected(token, "boolean")
  }

  /**
   * reads a long value.  Throws a
   * JsonParseException if the current value isn't a long
   */
  def readLong(): Long = next() match {
    case ValueLong(rv) => rv
    case token => unexpected(token, "integer")
  }

  /**
   * reads a long value or null.  Throws a
   * JsonParseException if the current value isn't a long
   */
  def readLongOption(): Option[Long] = next() match {
    case ValueNull => None
    case ValueLong(rv) => Some(rv)
    case token => unexpected(token, "integer")
  }

  /**
   * reads an int value.  Throws a
   * JsonParseException if the current value isn't a long
   */
  def readInt(): Int = next() match {
    case ValueLong(rv) => rv.toInt
    case token => unexpected(token, "integer")
  }

  /**
   * reads an int value or null.  Throws a
   * JsonParseException if the current value isn't a long
   */
  def readIntOption(): Option[Int] = next() match {
    case ValueNull => None
    case ValueLong(rv) => Some(rv.toInt)
    case token => unexpected(token, "integer")
  }

  /**
   * reads a double value.  Throws a
   * JsonParseException if the current value isn't a double or a long
   */
  def readDouble(): Double = next() match {
    case ValueDouble(rv) => rv
    case ValueLong(rv) => rv
    case token => unexpected(token, "double")
  }

  /**
   * reads a double value or null.  Throws a
   * JsonParseException if the current value isn't a double or a long
   */
  def readDoubleOption(): Option[Double] = next() match {
    case ValueNull => None
    case ValueDouble(rv) => Some(rv)
    case ValueLong(rv) => Some(rv)
    case token => unexpected(token, "double")
  }

  /**
   * Reads the entire next value as JSON encoded string.
   */
  def readNextAsJsonString() = {
    val buf = new StringWriter
    val out = Streamy.factory.createJsonGenerator(buf)
    def copyNext() {
      next() match {
        case ValueNull =>
          out.writeNull()
        case ValueBoolean(v) =>
          out.writeBoolean(v)
        case ValueLong(v) =>
          out.writeNumber(v)
        case ValueDouble(v) =>
          out.writeNumber(v)
        case ValueString(v) =>
          out.writeString(v)
        case StartObject =>
          out.writeStartObject()
          readObjectBody {
            case name =>
              out.writeFieldName(name)
              copyNext()
          }
          out.writeEndObject()
        case StartArray =>
          out.writeStartArray()
          readArrayBody(_ => copyNext())
          out.writeEndArray()
        case token => unexpected(token, "something something")
      }
    }
    copyNext()
    out.flush()
    buf.toString
  }

  /**
   * Skips past the next value (not just the next token), fast-forwarding to
   * an object or array end if at an object or array start.
   */
  def skipNext() {
    next()
    skipCurrent()
  }

  /**
   * Skips past the current value (not just the current token), fast-forwarding to
   * an object or array end if at an object or array start.
   */
  def skipCurrent() {
    current match {
      case StartObject => skipToObjectEnd()
      case StartArray => skipToArrayEnd()
      case _ => // nothing to do
    }
  }

  /**
   * Fast-forwards to the first EndObject at the same level or a higher
   * (less-nested) level.
   */
  def skipToObjectEnd() {
    while (next() != EndObject) skipCurrent()
  }

  /**
   * Fast-forwards to the first array end at the same level or a higher
   * (less-nested) level.
   */
  def skipToArrayEnd() {
    while (next() != EndArray) skipCurrent()
  }

  def unexpected(token: StreamyToken, expecting: String) = {
    token match {
      case NotAvailable => throw new JsonParseException("Unexpected end of input", location)
      case _ => throw new JsonParseException("Expecting " + expecting + ", found " + token, location)
    }
  }
}
