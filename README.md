StreamyJ
========

StreamyJ is a Scala helper for the Jackson streaming JSON parser.
Running some (likely entirely unreliable) benchmarks shows Jackson
to be significantly faster than GSON, which seems to be the most
popular non-streaming JSON parser available.

StreamyJ makes it easy to write idiomatic Scala parsers using Jackson by

*   Converting JsonToken constants to case classes, allowing pattern matching
*   Providing a mechanism to provide partial functions to the parser to take action
on specific parsed items.

It's easiest to show with an example

    val s = new Streamy("""{"bar":1}""")
    s.obj {
      case FieldName("bar") => println(s.readField())
    }

In this case, we call obj() on the Streamy object to tell the parser to read the current object.  
The case statements tell Streamy to print the current field value when the field name is "bar".
Simple enough.

A slightly more complex example

    var baz = 0L
    val s = new Streamy("""{"bar":{"baz":1}}""")
    s.obj {
      case FieldName("bar") => {
        s.obj {
          case FieldName("baz") => baz = s.readLongField()
        }
      }
    }

Here we have nested handlers.  When the parser encounters the "bar" field, we tell it
to read another object.  In this nested object, when you see the baz field, assign our
baz var whatever the value of the baz field is in JSON.

Parsing arrays is also handled.

    val s = new Streamy("""{"bar":[1,2,3]}""")
    s.obj {
      case FieldName("bar") => {
        s.arr {
          case ValueLong(l) => println(l)
        }
      }
    }

Parsing methods can also be defined and used without the {} syntax

    val printfield:Streamy.ParseFunc = {
      case FieldName(s) => println("hit field " + s)
    }
    val s = new Streamy("""{"bar":1, "baz":2}""")
    s.obj(printfield)

Lastly, if the helpers fail you always have access to the underlying Json reader,
as well as the current Streamy token

    val s = new Streamy("""{"bar": 1, "baz": {"baz2": 3}}""")
    s.obj {
      case FieldName("bar") => println("bar is " + s.readLongField())
      case FieldName("baz") => {
        println(s.next())
        s.token match {
          case StartObject => println("I'm at an object start")
          case _ => println("I'm not at an object start")
        }
        s.readObject(s.eat)
      }
    }
    