import scalaz.concurrent.Task
import scalaz.stream._
import java.io._


//Source.fromURL

object ahhh {

 def main(args: Array[String]) = {
   val buffer = new ByteArrayOutputStream
   val in: Process[Task,Int] = Process.unfold(0) { i =>
     println(s"gen $i")
     if (i < 30) Some(i, i+1) else None
   }
   val out : Process[Task, Array[Byte] => Task[Unit]] = io.chunkW(System.out)

   val s = in
     .map { i =>
       println(s"str $i")
       i.toString
     }
     .map { s =>
       println(s"nln $s")
       s + "\n"
     }
     .map { s =>
       print(s"pre $s")
       s"out $s"
     }
     .map { s =>
        print(s"byt $s")
        s.getBytes
      }
     .to(out)
   //.connect(out)(wye.boundedQueue(20))
   s.run.run
 }

}

//-----
//
//
//
//
//val in = Process.emitAll((1 to 10).map(_ => "http://www.globo.com").toSeq)
//
//
//in.map()
//
//val buffer = new ByteArrayOutputStream
//val out : Process[Task, Array[Byte] => Task[Unit]] = io.chunkW(buffer)