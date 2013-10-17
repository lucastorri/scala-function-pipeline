package co.torri.pipeline

import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.ActorSystem

object Example {

  def main(args: Array[String]) = {
    val p = Pipeline[Int]
      .mapM(10) { i =>
        Thread.sleep(3000)
        if (i == 5) throw new Exception
        i.toString
      }
      .map { s =>
        s + "!"
      }

    val a = new AutoOutput[String] {
      def apply(s: String) = println(s"chegou $s")
      def error(t: Throwable) = t.printStackTrace()
    }
    val ar = p.pipe(a)
    (0 until 50).foreach(ar.apply)

    {
      implicit val system = ActorSystem()
      val fr = p.pipe
      val f = fr(100)
      f.onSuccess { case v => println(s"success $v") }
    }

    val o = new Output[String] {
      def apply(s: String, continue: () => Unit) = { println(s); continue() }
      def error(t: Throwable, continue: () => Unit) = { t.printStackTrace(); continue() }
    }
    val or = p.pipe(o)(ActorSystem())
    (0 until 50).foreach(or.apply)
  }

}
