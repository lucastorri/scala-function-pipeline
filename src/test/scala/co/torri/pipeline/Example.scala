package co.torri.pipeline

import scala.concurrent.ExecutionContext.Implicits.global

object Example {

  def main(args: Array[String]) = {
    val p = Pipeline[Int]
      .map(10) { i =>
        Thread.sleep(3000)
        if (i == 5) throw new Exception
        i.toString
      }
      .map { s =>
        s + "!"
      }
    val o = new Output[String] {
      def apply(s: String) = println(s"chegou $s")
      def error(e: Exception) = e.printStackTrace()
    }
    val cr = p.pipe(o)
    (0 until 50).foreach(cr.apply)

    val fr = p.pipe
    val f = fr(100)
    f.onSuccess { case v => println(s"success $v") }
  }

}
