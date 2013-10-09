import akka.actor.{Props, ActorSystem, ActorRef, Actor}
import concurrent.{Future, Promise}
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global

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
    val cr = p.pipe(o)
    (0 until 10).foreach(cr.apply)

    val fr = p.pipe
    val f = fr(100)
    f.onSuccess { case v => println(s"success $v") }
  }
}

package object pipeline {
  val system = ActorSystem()

  def debug(a: Any) = {}//println(a)

  trait Pipeline[I, O] {
    def map[N](parallelism: Int)(f: O => N) : Pipeline[I, N]
    def map[N](f: O => N) : Pipeline[I, N] = map(1)(f)

    def pipe() : FutureRunner[I, O]
    def pipe(o: Output[O]) : CallbackRunner[I, O]
  }
  case class Stage[I, O] private[pipeline](stages: List[Func]) extends Pipeline[I, O] {

    def map[N](parallelism: Int)(f: O => N) : Pipeline[I, N] = {
      Stage(Func(parallelism, f) :: stages)
    }

    def pipe() : FutureRunner[I, O] = new FutureRunner[I, O] {
      private[this] val start = runner(Output.noop[O])

      def apply(v: I) = {
        val promise = Promise[O]()
        start ! TracedValue(v, v, promise)
        promise.future
      }
    }

    def pipe(output: Output[O]) : CallbackRunner[I, O] = new CallbackRunner[I, O] {
      private[this] val start = runner(output)

      def apply(v: I) = {
        start ! Value(v)
      }
    }

    private[this] def runner(output: Output[O]) : ActorRef = {
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

  }
  object Pipeline {
    def apply[I]() : Pipeline[I, I] = new Stage(List())
  }

  trait CallbackRunner[I, O] {
    def apply(v: I)
  }
  trait FutureRunner[I, O] {
    def apply(i: I) : Future[O]
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
    private[this] var values = mutable.ListBuffer.empty[Content[In]]
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
      case v: Content[In] =>
        debug(s"in $v")
        values += v
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
      case TracedValue(v: Out, original, output) =>
        debug(s"traced $v")
        output.success(v)
        before ! Pull()
      case Error(e) =>
        out.error(e)
        before ! Pull()
      case TracedError(e, original) =>
        ()
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
      case c: Content[In] =>
        waiting.head ! c
        waiting = waiting.tail
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
      case c: Content[In] =>
        debug(s"work $c")
        result = c.map(f)
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

  sealed trait Content[+C] {
    def map[N](f: C => N) : Content[N]
  }
  case class Value[V](v: V) extends Content[V] {
    def map[N](f: V => N) : Content[N] =
      try Value(f(v)) catch { case e: Exception => Error(e) }
  }
  case class Error(e: Exception) extends Content[Nothing] {
    def map[N](f: Nothing => N) : Content[N] = this
  }
  case class TracedValue[V, I, O](v: V, original: I, output: Promise[O]) extends Content[V] {
    def map[N](f: V => N) : Content[N] =
      try TracedValue(f(v), original, output)
      catch {
        case e: Exception =>
          output.failure(e)
          TracedError(e, original)
      }
  }
  case class TracedError[I](e: Exception, original: I) extends Content[Nothing] {
    def map[N](f: Nothing => N) : Content[N] = this
  }

  object Done
  object Free
  case class Pull(qty: Int)
  object Pull1 extends Pull(1)
  object Pull {
    def apply() : Pull = Pull1
  }
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