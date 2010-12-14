StreamyJ
========

StreamyJ is a Scala helper for the Jackson streaming JSON parser.
Running some (likely entirely unreliable) benchmarks shows Jackson
to be significantly faster than GSON, which seems to be the most
popular non-streaming JSON parser available.

THIS CODE IS DRAFT QUALITY AT BEST.  I've run tests with Twitter JSON
formats, but the testing is by no means thorough.  There are almost
certainly dire bugs lurking.

StreamyJ makes it easy to write idiomatic Scala parsers using Jackson by

*   Converting JsonToken constants to case classes, allowing pattern matching
*   Providing a mechanism to provide partial functions to the parser to take action
on specific parsed items.

It's easiest to show with an example

    val s = new Streamy("""{"bar":1}""")
    s readObject {
      case "bar" => println(s.readScalar())
    }

In this case, we call readObject() on the Streamy object to tell the parser to read the current object.
The case statements tell Streamy to print the current field value when the field name is "bar".
Simple enough.

A slightly more complex example

    var baz = 0L
    val s = new Streamy("""{"bar":{"baz":1}}""")
    s readObject {
      case "bar" => s readObject {
        case "baz" => baz = s.readLong()
      }
    }

Here we have nested handlers.  When the parser encounters the "bar" field, we tell it
to read another object.  In this nested object, when you see the baz field, assign our
baz var whatever the value of the baz field is in JSON.

Parsing arrays is also handled.  Instead of field names when reading an object,
the readArray takes a function that takes the current index in the array.

    val s = new Streamy("""{"bar":[1,2,3]}""")
    s readObject {
      case "bar" => s readArray {
        idx => println(idx + ": " + s.readLong())
      }
    }

Parsing methods can also be defined and used without the {} syntax

    val printfield: Streamy.ObjectParseFunc = {
      case s => println("hit field " + s)
    }
    val s = new Streamy("""{"bar":1, "baz":2}""")
    s.readObject(printfield)

