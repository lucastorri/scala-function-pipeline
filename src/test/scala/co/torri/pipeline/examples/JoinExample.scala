package co.torri.pipeline


object JoinExample {

  def main(args: Array[String]) = {

    implicit val execution = scala.concurrent.ExecutionContext.Implicits.global

    val p1 = Pipeline[String]
      .map(_ + "!")
      .future

    val p2 = Pipeline[Int]
      .map(_ + 100)
      .future

    val join = Pipeline(p1, p2)
      .foreach(println)


    List("a", "b", "c").foreach(p1.apply)
    List(1,2,3).foreach(p2.apply)
  }
}
