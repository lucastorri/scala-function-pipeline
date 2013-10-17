package co.torri.pipeline

import io.Source
import java.net.URLEncoder
import org.json4s._

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
      "http://www.github.com/",
      "http://www.twitter.com/",
      "http://www.scala-lang.org/",
      "http://www.oracle.com/",
      "http://www.amazon.com/"
    )

    val content : Pipeline[String, WebsiteContent] = Pipeline[String]
      .mapM(4) { url =>
        try {
          val size = Source.fromURL(url).size
          //println(s"content $url = $size")

          (url, size)
        } catch {
          case e: Exception =>
            (url, 0)
        }
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

        //println(social.mkString)
        //val json = parse(social.mkString)
        //val facebookLikes = (json \ "Facebook" \ "like_count").extract[Int]
        //val googlePlusOne = (json \ "GooglePlusOne").extract[Int]
        //val tweets = (json \ "Twitter").extract[Int]

        WebsiteSocialInfo(url, /*facebookLikes*/ 0, /*googlePlusOne*/ 0, /*tweets*/ 0)
      }

    val mainPipeline = Pipeline[String]
      .forkM2(4)(content, info)
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
