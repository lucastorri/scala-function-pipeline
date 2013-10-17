package co.torri.pipeline

import akka.actor.{Props, ActorRef, Actor}
import collection.mutable
import concurrent.Future
import util.{Failure, Success}

package object actors {

  case class Bind(stages: List[ActorRef])

  object Done
  object Free
  case class Pull(qty: Int)
  object Pull1 extends Pull(1)
  object Pull {
    def apply() : Pull = Pull1
  }
  case class Push(receiver: ActorRef)


  trait Start extends Actor {

    type In
    private[this] var after: ActorRef = _
    private[this] var values = mutable.ListBuffer.empty[Content[In]]
    private[this] var tokens = 0

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
        tokens += qty
        pushValues()
      case v: Content[In] =>
        debug(s"in $v")
        values += v
        pushValues()
    }

    def pushValues() {
      if (tokens > 0 && values.nonEmpty) {
        val push = math.min(tokens, values.size)
        values.take(push).foreach(after ! _)
        values = values.drop(push)
        tokens -= push
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
      case Value(v: Out, original, output) =>
        debug(s"traced $v")
        output.success(v)
        out(v, continue)
      case Error(t, original, output) =>
        output.failure(t)
        out.error(t, continue)
    }

    private[this] def continue = {
      lazy val x = before ! Pull()
      () => { x }
    }
  }

  trait Supervisor extends Actor {

    type In
    type Out

    def f : In => Future[Out]
    def parallelism: Int

    private[this] var before : ActorRef = _
    private[this] var after : ActorRef = _
    private[this] var waiting = mutable.ListBuffer.empty[ActorRef]
    private[this] var finished = mutable.ListBuffer.empty[ActorRef]
    private[this] var tokens = 0

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
        tokens += qty
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
      if (finished.nonEmpty && tokens > 0) {
        val push = math.min(finished.size, tokens)
        finished.take(push).foreach(_ ! Push(after))
        finished = finished.drop(push)
        tokens -= push
      }
    }

  }

  trait Worker extends Actor {

    type In
    type Out
    def f : In => Future[Out]
    private[this] var result: Content[Out] = _
    import context._

    def receive = free

    def free : Receive = {
      case v @ Value(value: In, original, output) =>
        val s = sender
        try {
          debug(s"worker $value")
          f(value).onComplete {
            case Success(newValue: Out) =>
              debug(s"worker $value => $newValue")
              continue(s, v.next[Out](newValue))
            case Failure(t) =>
              t.printStackTrace
              continue(s, v.fail(t))
          }
        } catch {
          case t: Throwable =>
            continue(s, v.fail(t))
        }

      case e @ Error(t, original, output) =>
        continue(sender, e)
    }

    private[this] def continue(sender: ActorRef, c: Content[Out]){
      result = c
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

}
