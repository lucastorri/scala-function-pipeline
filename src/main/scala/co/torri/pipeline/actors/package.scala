package co.torri.pipeline

import akka.actor.{Props, ActorRef, Actor}
import collection.mutable
import concurrent.{Promise, Future}
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

  case class JoinStart[Out, Final](f: () => Future[Out])(implicit opts: Opts) extends Actor {

    private[this] var after: ActorRef = _

    import context._

    def receive = {
      case Bind(stages) =>
        debug(s"Start: Bind($stages)")
        after = stages.head
        after ! Bind(stages.tail)
        become(up)
    }

    def up : Receive = {
      case Pull(qty) =>
        debug(s"Start: Pull($qty)")
        (1 to qty).foreach { _ =>
          try {
            f().onComplete {
              case Success(v) =>
                after ! Value[Out, Null, Final](v, null, Promise())
              case Failure(t) =>
                after ! Error[Null, Final](t, null, Promise())
            }
          } catch {
            case t: Throwable =>
              after ! Error[Null, Final](t, null, Promise())
          }
        }
    }

  }


  case class Start[In]()(implicit opts: Opts) extends Actor {

    private[this] var after: ActorRef = _
    private[this] var values = mutable.ListBuffer.empty[Content[In]]
    private[this] var tokens = 0

    import context._

    def receive = {
      case Bind(stages) =>
        debug(s"Start: Bind($stages)")
        after = stages.head
        after ! Bind(stages.tail)
        become(up)
    }

    def up : Receive = {
      case Pull(qty) =>
        debug(s"Start: Pull($qty)")
        tokens += qty
        pushValues()
      case v: Content[In] =>
        debug(s"Start: Content($v)")
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

  class WaitEnd[O]()(implicit opts: Opts) extends Actor {

    private[this] var before: ActorRef = _
    private[this] var waiting = mutable.ListBuffer.empty[Promise[O]]

    import context._

    def receive = {
      case Bind(stages) =>
        debug(s"WaitEnd: receive/Bind($stages)")
        before = sender
        become(up)
    }

    def up: Receive = {
      case p: Promise[O] =>
        debug(s"WaitEnd: up/Promise($p)")
        waiting += p
        before ! Pull()
      case c: Content[O] =>
        debug(s"WaitEnd: up/Content($c)")
        waiting.head.complete(c.toTry)
        waiting = waiting.drop(1)
    }

  }

  class CommitEnd()(implicit opts: Opts) extends Actor {

    private[this] var before: ActorRef = _

    import context._

    def receive = {
      case Bind(stages) =>
        debug(s"CommitEnd: Bind($stages)")
        before = sender
        before ! Pull()
        become(up)
    }

    def up : Receive = {
      case c: Content[_] =>
        debug(s"CommitEnd: Content($c)")
        c.commit()
        before ! Pull()
      case m =>
        debug(s"CommitEnd: Unknown($m)")
    }

  }

  case class Supervisor[In, Out](id: Int, f: Content[In] => Future[Out], parallelism: Int)(implicit opts: Opts) extends Actor {

    private[this] var before : ActorRef = _
    private[this] var after : ActorRef = _
    private[this] var waiting = mutable.ListBuffer.empty[ActorRef]
    private[this] var finished = mutable.ListBuffer.empty[ActorRef]
    private[this] var tokens = 0

    import context._

    def receive = {
      case Bind(stages) =>
        debug(s"Supervisor#$id: Bind($stages)")
        before = sender
        after = stages.head
        after ! Bind(stages.tail)
        waiting = mutable.ListBuffer.range(0, parallelism).map(workerId => actorOf(Props(creator = () => Worker[In, Out](workerId, id, f))))
        become(free)
        before ! Pull(parallelism)
    }

    def free : Receive = {
      case Pull(qty) =>
        debug(s"Supervisor#$id: Pull($qty)")
        tokens += qty
        pushWork()
      case Free =>
        debug(s"Supervisor#$id: Free($sender)")
        finished += sender
        pushWork()
      case Done =>
        debug(s"Supervisor#$id: Done($sender)")
        before ! Pull()
        waiting += sender
      case c: Content[In] =>
        debug(s"Supervisor#$id: Content($c)")
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

  case class Worker[In, Out](id: Int, supervisorId: Int, f : Content[In] => Future[Out])(implicit opts: Opts) extends Actor {

    private[this] var result: Content[Out] = _
    import context._

    def receive = free

    def free : Receive = {
      case c: Content[In] =>
        val s = sender
        try {
          debug(s"Worker#$supervisorId-$id: Content($c)")
          f(c).onComplete {
            case Success(newValue) =>
              debug(s"Worker#$supervisorId-$id: Mapped($c => $newValue)")
              continue(s, c.next[Out](newValue))
            case Failure(t) =>
              continue(s, c.fail(t))
          }
        } catch {
          case t: Throwable =>
            continue(s, c.fail(t))
        }
    }

    private[this] def continue(sender: ActorRef, c: Content[Out]){
      result = c
      sender ! Free
      become(finished)
    }

    def finished : Receive = {
      case Push(receiver) =>
        debug(s"Worker#$supervisorId-$id: Push($receiver)")
        receiver ! result
        sender ! Done
        result = null
        become(free)
    }

  }

}
