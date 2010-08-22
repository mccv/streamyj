import sbt._

class StreamyJProject(info: ProjectInfo) extends DefaultProject(info)
{
  val jackson = "org.codehaus.jackson" % "jackson-core-asl" % "1.5.2"
  val specs = "org.scala-tools.testing" % "specs" % "1.6.2.1"
  val objenesis = "org.objenesis" % "objenesis" % "1.1"
}
