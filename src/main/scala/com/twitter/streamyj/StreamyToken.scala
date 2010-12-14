package com.twitter.streamyj

import org.codehaus.jackson._
import org.codehaus.jackson.JsonToken._

/**
 * The base case class for a JsonToken
 */
abstract sealed trait StreamyToken
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
/** maps to JsonToken.VALUE_NULL */
case object ValueNull extends StreamyToken
/** A base class for long, double, and string fields */
abstract class ValueScalar extends StreamyToken {
  def value: Any
}
/** maps to JsonToken.VALUE_FALSE and VALUE_TRUE */
case class ValueBoolean(val value: Boolean) extends ValueScalar
/** maps to JsonToken.VALUE_NUMBER_INT */
case class ValueLong(val value: Long) extends ValueScalar
/** maps to JsonToken.VALUE_NUMBER_FLOAT */
case class ValueDouble(val value: Double) extends ValueScalar
/** maps to JsonToken.VALUE_STRING */
case class ValueString(val value: String) extends ValueScalar

object StreamToken {
  def apply(parser: JsonParser) = {
    parser.nextToken() match {
      case START_ARRAY => StartArray
      case END_ARRAY => EndArray
      case START_OBJECT => StartObject
      case END_OBJECT => EndObject
      case FIELD_NAME => FieldName(parser.getCurrentName)
      case NOT_AVAILABLE => NotAvailable
      case VALUE_FALSE => ValueBoolean(false)
      case VALUE_TRUE => ValueBoolean(true)
      case VALUE_NULL => ValueNull
      case VALUE_NUMBER_FLOAT => ValueDouble(parser.getDoubleValue())
      case VALUE_NUMBER_INT => ValueLong(parser.getLongValue())
      case VALUE_STRING => ValueString(parser.getText())
      case _ => NotAvailable
    }
  }
}