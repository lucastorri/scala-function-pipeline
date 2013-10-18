package co.torri.pipeline


object Example {

  implicit val execution = scala.concurrent.ExecutionContext.Implicits.global

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
      .foreach(println)

    (1 to 30).foreach(p.apply)
  }

}
