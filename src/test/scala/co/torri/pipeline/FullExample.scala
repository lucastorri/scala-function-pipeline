package co.torri.pipeline

import io.Source
import java.net.URLEncoder
import org.json4s._
import org.json4s.native.JsonMethods._

object FullExample {

  implicit val execution = scala.concurrent.ExecutionContext.Implicits.global
  implicit val formats = DefaultFormats

  case class WebsiteContent(
    url: String,
    pageSize: Int
  )

  case class WebsiteSocialInfo(
    url: String,
    facebookLikes: Int,
    googlePlusOne: Int,
    tweets: Int
  )

  def main(args: Array[String]) = {

    val websites = Seq(
      "http://www.nokia.com/",
      "http://www.google.com/",
      "http://www.twitter.com/",
      "http://facebook.com",
      "http://www.github.com/",
      "http://www.scala-lang.org/",
      "http://www.oracle.com/",
      "http://www.amazon.com/"
    )

    val content : Pipeline[String, WebsiteContent] = Pipeline[String]
      .mapM(4) { url =>
        try (url, Source.fromURL(url).size)
        catch { case e: Exception => (url, 0) }
      }
      .map { case (url, content) =>
        WebsiteContent(url, content)
      }

    val info : Pipeline[String, WebsiteSocialInfo] = Pipeline[String]
      .mapM(4) { url =>
        val encoded = URLEncoder.encode(url, "utf8")
        (url, Source.fromURL(s"http://api.sharedcount.com/?url=$encoded"))
      }
      .map { case (url, social) =>
        val json = parse(social.mkString)
        val facebookLikes = (json \ "Facebook" \ "like_count").extractOrElse[Int](0)
        val googlePlusOne = (json \ "GooglePlusOne").extractOrElse[Int](0)
        val tweets = (json \ "Twitter").extractOrElse[Int](0)

        WebsiteSocialInfo(url, facebookLikes, googlePlusOne, tweets)
      }

    val mainPipeline = Pipeline[String]
      .fork(4, content, info)
      .map { case (url, c, i) =>

        println(s"${url} = ${c.pageSize} bytes")
        println(s"\tFacebook Likes: ${i.facebookLikes}")
        println(s"\tGoogle +1: ${i.googlePlusOne}")
        println(s"\tTweets: ${i.tweets}")

      }
      .pipe

    websites.foreach(mainPipeline.apply)
  }

}
