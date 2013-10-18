package co.torri.pipeline

import org.json4s.DefaultFormats

object ForeverExample {

  implicit val execution = scala.concurrent.ExecutionContext.Implicits.global
  implicit val formats = DefaultFormats

  type X = List[Symbol]

  def main(args: Array[String]) = {

    def nop(a: Any) = {}
    def p(a: Any) = println(s">>> $a")

    val fork1 = pipeline('fork1)
      .future
    val fork2 = pipeline('fork2)
      .future

    val join = pipeline('join)
      .join(fork1, fork2)
      .foreach(p)

    val forkJoin1 = pipeline('forkJoin1)
      .fork(fork1)
      .future
    val forkJoin2 = pipeline('forkJoin2)
      .fork(fork2)
      .future

    val start = pipeline('start)
      .map { l =>
        Thread.sleep(1000); l
      }
      .forkJoin(forkJoin1, forkJoin2)
      .foreach(p)

    (1 to 3).foreach { i =>
      val l = List(Symbol(i.toString))
      join(l)
      start(l)
    }
  }


  def pipeline(s: Symbol)(implicit opts: Opts = Pipeline.defaultOpts) : Pipeline[X, X] =
    Pipeline[X]
      .map { s :: _ }

}
