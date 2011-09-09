import sbt._
import com.twitter.sbt._

class StreamyJProject(info: ProjectInfo) extends StandardProject(info)
with SubversionPublisher
with PublishSourcesAndJavadocs
with PublishSite
{
  val jackson = "org.codehaus.jackson" % "jackson-core-asl" % "1.8.1"
  val objenesis = "org.objenesis" % "objenesis" % "1.1"
  val specs     = buildScalaVersion match {
    case "2.7.7" => "org.scala-tools.testing" % "specs" % "1.6.2.1" % "test"
    case _ => "org.scala-tools.testing" %% "specs" % "1.6.6" % "test"
  }

  override def disableCrossPaths = false

  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public")
}
