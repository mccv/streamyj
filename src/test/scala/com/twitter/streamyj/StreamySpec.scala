package com.twitter.streamyj

import java.util.HashSet
import org.codehaus.jackson._
import org.specs._
import scala.collection.mutable.ListBuffer

case class Geo(latitude: Double, longitude: Double)
case class Place(x1: Double, y1: Double, x2: Double, y2: Double)

case class AnnotationAttribute(name: String, value: String) {
  val trackKeys = buildTrackKeys()

  def buildTrackKeys() = {
    val s = new HashSet[String]()
    s.add("*=*")
    s.add(name + "=*")
    s.add("*=" + value)
    s.add(name + "=" + value)
    s
  }
}

case class Annotation(typeName: String, attributes: Seq[AnnotationAttribute]) {
  val trackKeys = buildTrackKeys()

  def buildTrackKeys() = {
    val s = new HashSet[String]()
    attributes.foreach {attr =>
      val i = attr.trackKeys.iterator
      while (i.hasNext()) {
        val attrKey = i.next()
        s.add("*:" + attrKey)
        s.add(typeName + ":" + attrKey)
      }
    }
    s
  }
}

object StreamySpec extends Specification {
  var kind = -1

  var userIdOpt:Option[Long] = None
  var retweetUserIdOpt:Option[Long] = None
  var statusIdOpt:Option[Long] = None
  var limitTrackOpt:Option[Long] = None
  var geoOpt:Option[Geo] = None
  var placeOpt:Option[Place] = None
  var sourceOpt:Option[String] = None
  var inReplyToUserIdOpt:Option[Long] = None
  var idOpt:Option[Long] = None
  var retweetIdOpt:Option[Long] = None
  var createdAtOpt:Option[String] = None
  var textOpt:Option[String] = None
  var annotationsOpt:Option[Seq[Annotation]] = None

  val annotatedJSON = """{"geo":null,"in_reply_to_user_id":null,"favorited":false,"annotations":[{"amazon":{"price":"$49.99","id":"4"}},{"book":{"author":"Dickens","title":"Some Dickens Thing"}}],"text":"an annotated tweet","created_at":"Sat Apr 24 00:00:00 +0000 2010","truncated":false,"coordinates":null,"in_reply_to_screen_name":null,"contributors":null,"user":{"geo_enabled":false,"profile_background_tile":false,"profile_background_color":"9ae4e8","notifications":false,"lang":"en","following":false,"profile_text_color":"000000","utc_offset":-28800,"created_at":"Sat Apr 24 00:00:00 +0000 2010","followers_count":2,"url":null,"statuses_count":1,"profile_link_color":"0000ff","profile_image_url":"http://s3.amazonaws.com/twitter_development/profile_images/2/jack_normal.jpg","friends_count":2,"contributors_enabled":false,"time_zone":"Pacific Time (US & Canada)","profile_sidebar_fill_color":"e0ff92","protected":true,"description":"love, love","screen_name":"jack","favourites_count":0,"name":"Jack","profile_sidebar_border_color":"87bc44","id":3,"verified":false,"profile_background_image_url":"/images/themes/theme1/bg.png","location":"San Francisco"},"place":null,"in_reply_to_status_id":null,"source":"web","id":1234}"""
  val unAnnotatedJSON = """{"geo":null,"in_reply_to_user_id":null,"favorited":false,"text":"an annotated tweet","created_at":"Sat Apr 24 00:00:00 +0000 2010","truncated":false,"coordinates":null,"in_reply_to_screen_name":null,"contributors":null,"user":{"geo_enabled":false,"profile_background_tile":false,"profile_background_color":"9ae4e8","notifications":false,"lang":"en","following":false,"profile_text_color":"000000","utc_offset":-28800,"created_at":"Sat Apr 24 00:00:00 +0000 2010","followers_count":2,"url":null,"statuses_count":1,"profile_link_color":"0000ff","profile_image_url":"http://s3.amazonaws.com/twitter_development/profile_images/2/jack_normal.jpg","friends_count":2,"contributors_enabled":false,"time_zone":"Pacific Time (US & Canada)","profile_sidebar_fill_color":"e0ff92","protected":true,"description":"love, love","screen_name":"jack","favourites_count":0,"name":"Jack","profile_sidebar_border_color":"87bc44","id":3,"verified":false,"profile_background_image_url":"/images/themes/theme1/bg.png","location":"San Francisco"},"place":null,"in_reply_to_status_id":null,"source":"web","id":1234}"""
  val blankAnnotatedJSON = """{"geo":null,"in_reply_to_user_id":null,"favorited":false,"annotations":[],"text":"an annotated tweet","created_at":"Sat Apr 24 00:00:00 +0000 2010","truncated":false,"coordinates":null,"in_reply_to_screen_name":null,"contributors":null,"user":{"geo_enabled":false,"profile_background_tile":false,"profile_background_color":"9ae4e8","notifications":false,"lang":"en","following":false,"profile_text_color":"000000","utc_offset":-28800,"created_at":"Sat Apr 24 00:00:00 +0000 2010","Followers_count":2,"url":null,"statuses_count":1,"profile_link_color":"0000ff","profile_image_url":"http://s3.amazonaws.com/twitter_development/profile_images/2/jack_normal.jpg","friends_count":2,"contributors_enabled":false,"time_zone":"Pacific Time (US & Canada)","profile_sidebar_fill_color":"e0ff92","protected":true,"description":"love, love","screen_name":"jack","favourites_count":0,"name":"Jack","profile_sidebar_border_color":"87bc44","id":3,"verified":false,"profile_background_image_url":"/images/themes/theme1/bg.png","location":"San Francisco"},"place":null,"in_reply_to_status_id":null,"source":"web","id":1234}"""

  "Streamy" should {
    "work" in {
      val s = new Streamy("""{"id":1, "bar":{"id":3, "bar":2}, "arr":[1,2,3], "text":"some text", "bool":true}""")
      s.next()

      var id1 = -1L
      var id3 = -1L
      var arr = List[Long]()
      var text = ""
      var bool = false

      val submatch:Streamy.ParseFunc = {
        case FieldName("id") => id3 = s.readLongField()
      }

      s.obj {
        case FieldName("id") => id1 = s.readLongField()
        case FieldName("text") => text = s.readStringField()
        case StartArray => {
          s.arr {
            case ValueLong(l) => arr = l :: arr
          }
        }
        case StartObject => {
          s.obj(submatch)
        }
        case FieldName("bool") => {
          s.next() match {
            case ValueFalse => bool = false
            case ValueTrue => bool = true
          }
        }
      }
      id1 must be_==(1)
      id3 must be_==(3)
      arr must be_==(List(3L,2L,1L))
      text must be_==("some text")
      bool must be_==(true)
    }

    "work with no parsing logic" in {
      val s = new Streamy("""{"id":1, "text":"text", "bar":{"id":3, "bar":2}, "foo":2}""")
      // just make sure this doesn't throw anything (e.g. stack overflow)
      s.obj(s.eat)
      true must be_==(true)
    }

    "handle null objects correctly" in {
      val s = new Streamy("""{"id":1, "text":"text", "bar":null, "foo":2}""")
      // just make sure this doesn't throw anything (e.g. stack overflow)
      var foo = 0L
      s \ {
        case FieldName("foo") => foo = s.readLongField()
      }
      foo must be_==(2)
    }

    "handle embedded objects and arrays" in {
      val s = new Streamy("""{"id":1, "embed":{"foo":"bar", "baz":{"baz":1}, "arr":[[1],2,3,4]},"id2":2}""")
      var id = 0L
      var id2 = 0L
      s \ {
        case FieldName("id") => id = s.readLongField()
        case FieldName("id2") => id2 = s.readLongField()
      }
      id must be_==(1)
      id2 must be_==(2)
    }

    "work on a tweet" in {
      val s = new Streamy(unAnnotatedJSON)

      s.obj {
        case FieldName("delete") => {
          kind = 1 // delete
          readDelete(s)
        }
        case FieldName("scrub_geo") => {
          kind = 2 // scrub geo
          readScrubGeo(s)
        }
        case FieldName("limit") => {
          kind = 3 // limit
          readLimit(s)
        }
        case FieldName("geo") => readGeo(s)
        case FieldName("place") => readPlace(s)
        case FieldName("retweeted_status") => readRetweet(s)
        case FieldName("user") => {
          readUser(s, false)
        }
        case FieldName("annotations") => readAnnotations(s)
        case FieldName("entities") => readEntities(s)
        case FieldName("id") => idOpt = Some(s.readLongField())
        case FieldName("source") => sourceOpt = Some(s.readStringField())
        case FieldName("created_at") => createdAtOpt = Some(s.readStringField())
        case FieldName("text") => textOpt = Some(s.readStringField())
        case FieldName("in_reply_to_user_id") => {
          val token = s.next()
          if (token != ValueNull) {
            inReplyToUserIdOpt = Some(s.readLongField())
          }
        }
      }
      userIdOpt must be_==(Some(3))                   
    }
    "work on a blank annotated tweet" in {
      val s = new Streamy(blankAnnotatedJSON)

      s.obj {
        case FieldName("delete") => {
          kind = 1 // delete
          readDelete(s)
        }
        case FieldName("scrub_geo") => {
          kind = 2 // scrub geo
          readScrubGeo(s)
        }
        case FieldName("limit") => {
          kind = 3 // limit
          readLimit(s)
        }
        case FieldName("geo") => readGeo(s)
        case FieldName("place") => readPlace(s)
        case FieldName("retweeted_status") => readRetweet(s)
        case FieldName("user") => {
          readUser(s, false)
        }
        case FieldName("annotations") => readAnnotations(s)
        case FieldName("entities") => readEntities(s)
        case FieldName("id") => idOpt = Some(s.readLongField())
        case FieldName("source") => sourceOpt = Some(s.readStringField())
        case FieldName("created_at") => createdAtOpt = Some(s.readStringField())
        case FieldName("text") => textOpt = Some(s.readStringField())
        case FieldName("in_reply_to_user_id") => {
          val token = s.next()
          if (token != ValueNull) {
            inReplyToUserIdOpt = Some(s.readLongField())
          }
        }
      }
      userIdOpt must be_==(Some(3))                   
    }
  }

  def readDelete(s: Streamy) = {
    s \ {
      case FieldName("status") => {
        s \ {
          case FieldName("user_id") => userIdOpt = Some(s.readLongField())
          case FieldName("id") => idOpt = Some(s.readLongField())
        }
      }
    }
  }
  
  def readLimit(s: Streamy) = {
    s \ {
      case FieldName("track") => limitTrackOpt = Some(s.readLongField())
    }
  }
  
  def readScrubGeo(s: Streamy) = {
    s \ {
      case FieldName("user_id") => userIdOpt = Some(s.readLongField())
      case FieldName("up_to_status_id") => idOpt = Some(s.readLongField())
    }
  }

  def readRetweet(s: Streamy) = {
    s \ {
      case FieldName("user") => readUser(s, true)
      case FieldName("id") => retweetIdOpt = Some(s.readLongField())
    }
  }

  def readUser(s: Streamy, isRetweet: Boolean) = {
    s \ {
      case FieldName("id") => {
        if (isRetweet) {
          retweetUserIdOpt = Some(s.readLongField())
        } else {
          userIdOpt = Some(s.readLongField())
        }
      }
    }
  }

  def readGeo(s: Streamy) = {
    s \ {
      case FieldName("coordinates") => {
        try {
          s arr {
            case ValueDouble(d) => {
              val latitude = s.readDoubleField()
              s.next()
              val longitude = s.readDoubleField()
              geoOpt = Some(Geo(latitude, longitude))
            }
          }
        } catch {
          case e: NumberFormatException => throw new IllegalArgumentException()
        }
      }
    }
  }

  def readPlace(s: Streamy) = {
    s \ {
      case FieldName("bounding_box") => {
        s \ {
          case FieldName("coordinates") => {
            var minX = Math.MAX_DOUBLE
            var minY = Math.MAX_DOUBLE
            var maxX = Math.MIN_DOUBLE
            var maxY = Math.MIN_DOUBLE
            // two nested arrays
            s arr {
              case StartArray => s arr {
                case ValueDouble(d) => {
                  val x = s.readDoubleField()
                  s.next()
                  val y = s.readDoubleField()
                  if (x < minX) minX = x
                  if (y < minY) minY = y
                  if (x > maxX) maxX = x
                  if (y > maxY) maxY = y
                }
              }
            }
            placeOpt = Some(Place(minX, minY, maxX, maxY))
          }
        }
      }
    }
  }

  def readAnnotations(s: Streamy) = {
    var parsedAnnotations = new ListBuffer[Annotation]()
    s arr {
      case StartObject => {
        s \ {
          case FieldName(typeName) => {
            var attributes = new ListBuffer[AnnotationAttribute]()
            s \ {
              case FieldName(attrName) => {
                attributes += AnnotationAttribute(attrName, s.readStringField())
              }
            }
            parsedAnnotations += Annotation(typeName, attributes)
          }
        }
      }
      annotationsOpt = Some(parsedAnnotations)
    }
  }

  def readEntities(s: Streamy) = {
    s \ {
      case FieldName("urls") => {
      }
      case FieldName("hashtags") => {
      }
    }
  }
  
}
