import akka.actor.{Props, ActorSystem, ActorRef, Actor}
import scala.collection.mutable

object m {
  import pipeline._

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
    val r = p.to(o)
    (0 until 10).foreach(r.apply)
  }
}

package object pipeline {
  val system = ActorSystem()

  def debug(a: Any) = {}//println(a)

  trait Pipeline[I, O] {
    def map[N](parallelism: Int)(f: O => N) : Pipeline[I, N]
    def map[N](f: O => N) : Pipeline[I, N] = map(1)(f)

    def to(o: Output[O]) : Runner[I]
  }
  case class Stage[I, O] private[pipeline](stages: List[Func]) extends Pipeline[I, O] {

    def map[N](parallelism: Int)(f: O => N) : Pipeline[I, N] = {
      Stage(Func(parallelism, f) :: stages)
    }

    def to(output: Output[O]) : Runner[I] = new Runner[I] {
      private[this] val start : ActorRef = {
        val start : ActorRef = system.actorOf(Props(creator = () => new Start {
          type In = I
        }))

        val end : ActorRef = system.actorOf(Props(creator = () => new End {
          type Out = O
        }))

        def supervisor(func: Func) : ActorRef = system.actorOf(Props(creator = () => new Supervisor {
          def parallelism: Int = func.p

          def f = func.f.asInstanceOf[In => Out]

          type In = func.In
          type Out = func.Out
        }))

        val actors : List[ActorRef] = (end :: stages.map(supervisor)).reverse
        end ! output
        start ! Bind(actors)
        start
      }

      def apply(v: I) = {
        start ! v
      }
    }

  }
  object Pipeline {
    def apply[I]() : Pipeline[I, I] = new Stage(List())
  }

  trait Runner[I] {
    def apply(v: I)
  }

  trait Func {
    type In
    type Out

    def p : Int
    def f : In => Out
  }
  object Func {
    def apply[I, O](parallelism: Int, ff: I => O) : Func = new Func {
      type In = I
      type Out = O

      val p = parallelism
      val f = ff
    }
  }

  trait Output[O] {
    def apply(v: O)
    def error(e: Exception)
  }
  object Output {
    def noop[O] = new Output[O]() {
      def apply(v: O) = {}
      def error(e: Exception) = {}
    }
  }

  trait Start extends Actor {

    type In
    private[this] var after: ActorRef = _
    private[this] var values = mutable.ListBuffer.empty[Value[In]]
    private[this] var next = 0

    import context._

    def receive = {
      case Bind(stages) =>
        debug("bind start")
        after = stages.head
        after ! Bind(stages.tail)
        become(up)
    }

    def up : Receive = {
      case Pull(qty) =>
        debug(s"next start $qty")
        next += qty
        pushValues()
      case v: In =>
        debug(s"in $v")
        values += Value(v)
        pushValues()
    }

    def pushValues() {
      if (next > 0 && values.nonEmpty) {
        val push = math.min(next, values.size)
        values.take(push).foreach(after ! _)
        values = values.drop(push)
        next -= push
      }
    }
  }

  trait End extends Actor {

    type Out
    private[this] var out: Output[Out] = Output.noop
    private[this] var before: ActorRef = _

    import context._

    def receive = {
      case o: Output[Out] =>
        debug("set out")
        out = o
        become(bindable)
    }

    def bindable : Receive = {
      case Bind(stages) =>
        before = sender
        before ! Pull()
        become(up)
    }

    def up : Receive = {
      case Value(v: Out) =>
        debug(s"out $v")
        out(v)
        before ! Pull()
      case Error(e) =>
        out.error(e)
    }
  }

  trait Supervisor extends Actor {

    type In
    type Out

    def f : In => Out
    def parallelism: Int

    private[this] var before : ActorRef = _
    private[this] var after : ActorRef = _
    private[this] var waiting = mutable.ListBuffer.empty[ActorRef]
    private[this] var finished = mutable.ListBuffer.empty[ActorRef]
    private[this] var next = 0

    import context._

    def receive = {
      case Bind(stages) =>
        debug("bind", stages)
        before = sender
        after = stages.head
        after ! Bind(stages.tail)
        waiting = mutable.ListBuffer.fill(parallelism)(actorOf(Props(creator = () => new Worker {
          type In = Supervisor.this.In
          type Out = Supervisor.this.Out
          val f = Supervisor.this.f
        })))
        become(free)
        before ! Pull(parallelism)
    }

    def free : Receive = {
      case Pull(qty) =>
        debug(s"next $qty")
        next += qty
        pushWork()
      case Free =>
        debug(s"free")
        finished += sender
        pushWork()
      case Done =>
        debug(s"done")
        before ! Pull()
        waiting += sender
      case v: Value[In] =>
        waiting.head ! v
        waiting = waiting.tail
      case e: Error =>
        after ! e
    }

    private[this] def pushWork() = {
      if (finished.nonEmpty && next > 0) {
        val push = math.min(finished.size, next)
        finished.take(push).foreach(_ ! Push(after))
        finished = finished.drop(push)
        next -= push
      }
    }

  }

  trait Worker extends Actor {

    type In
    type Out
    def f : In => Out
    private[this] var result: Content[_] = _
    import context._

    def receive = free

    def free : Receive = {
      case Value(v: In) =>
        debug(s"work $v")
        result =
          try Value(f(v))
          catch { case e: Exception => e.printStackTrace; Error(e) }

        sender ! Free
        become(finished)
    }

    def finished : Receive = {
      case Push(receiver) =>
        debug(s"push $receiver")
        receiver ! result
        sender ! Done
        result = null
        become(free)
    }

  }

  case class Bind(stages: List[ActorRef])

  trait Content[C]
  case class Value[V](v: V) extends Content[V]
  case class Error(e: Exception) extends Content[Exception] //TODO

  object Done
  object Free
  case class Pull(qty: Int = 1)
  case class Push(receiver: ActorRef)



}

class Flow[I, O](f: I => O) {
  def map[N](nf: O => N) : Flow[I, N] = new Flow(f.andThen(nf))
  def apply(v: I) : O = f(v)
}
object Flow {
  def apply[I]() : Flow[I, I] = new Flow[I, I](v => v)
}

case class Exec[T] private[Exec](values: Seq[T]) {
  def apply[O](f: Flow[T, O]) : Exec[O] = Exec(values.map(f.apply))
}
object Exec {
  def single[T](value: T) : Exec[T] = seq(Seq(value))
  def all[T](values: T*) : Exec[T] = seq(values.toSeq)
  def seq[T](seq: Seq[T]) : Exec[T] = new Exec(seq)
}

object e {
  val e = Exec.all(1,2,3,4,5,6,7,8,9,0)
  val f = Flow[Int]().map(_.toString).map(_ + "!")
}