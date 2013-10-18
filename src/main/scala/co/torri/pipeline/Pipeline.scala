package co.torri.pipeline

import co.torri.pipeline.actors._
import concurrent.{ExecutionContextExecutor, Future, Promise}
import akka.actor.{Actor, ActorSystem, Props, ActorRef}
import util.Try

trait Pipeline[I, O] {
  def fork(r1: Runner[O, _], rs: Runner[O, _]*)(implicit system: ActorSystem) : Pipeline[I, O]

  def join[N1](r1: FutureRunner[_, N1])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1)] = join(1, r1)
  def join[N1](parallelism: Int, r1: FutureRunner[_, N1])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1)]

  def join[N1, N2](r1: FutureRunner[_, N1], r2: FutureRunner[_, N2])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1, N2)] = join(1, r1, r2)
  def join[N1, N2](parallelism: Int, r1: FutureRunner[_, N1], r2: FutureRunner[_, N2])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1, N2)]

  def forkJoin[N1](parallelism: Int, f1: FutureRunner[O, N1])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1)]
  def forkJoin[N1](f1: FutureRunner[O, N1])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1)] = forkJoin(1, f1)

  def forkJoin[N1, N2](parallelism: Int, f1: FutureRunner[O, N1], f2: FutureRunner[O, N2])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1, N2)]
  def forkJoin[N1, N2](f1: FutureRunner[O, N1], f2: FutureRunner[O, N2])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1, N2)] = forkJoin(1, f1, f2)

  def mapM[N](parallelism: Int)(f: O => N) : Pipeline[I, N]
  def map[N](f: O => N) : Pipeline[I, N] = mapM(1)(f)

  def future()(implicit system: ActorSystem) : FutureRunner[I, O]
  def foreach(f: Try[O] => Unit)(implicit system: ActorSystem) : Runner[I, O] = foreachM(1)(f)
  def foreachM(parallelism: Int)(f: Try[O] => Unit)(implicit system: ActorSystem) : Runner[I, O]
}
case class Stage[I, O] private[pipeline](stages: List[Func], starter: Starter[I])(implicit opts: Opts) extends Pipeline[I, O] {

  def fork(r1: Runner[O, _], rs: Runner[O, _]*)(implicit system: ActorSystem) : Pipeline[I, O] = {
    val runners = (r1 :: rs.toList)

    addStep(1) { o =>
      runners.foreach(_(o))
      Future.successful(o)
    }
  }

  def join[N1](parallelism: Int, r1: FutureRunner[_, N1])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1)] = {
    addStep(parallelism) { o =>
      val n1 = r1.next
      for {
        o1 <- n1
      } yield (o, o1)
    }
  }

  def join[N1, N2](parallelism: Int, r1: FutureRunner[_, N1], r2: FutureRunner[_, N2])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1, N2)] = {
    addStep(parallelism) { o =>
      val n1 = r1.next
      val n2 = r2.next
      for {
        o1 <- n1
        o2 <- n2
      } yield (o, o1, o2)
    }
  }

  def forkJoin[N1](parallelism: Int, f1: FutureRunner[O, N1])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1)] = {

    addStep(parallelism) { o =>
      f1(o)
      for {
        o1 <- f1.next
      } yield (o, o1)
    }
  }
  def forkJoin[N1, N2](parallelism: Int, f1: FutureRunner[O, N1], f2: FutureRunner[O, N2])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1, N2)] = {
    addStep(parallelism) { o =>
      f1(o)
      f2(o)
      for {
        o1 <- f1.next
        o2 <- f2.next
      } yield (o, o1, o2)
    }
  }

  def mapM[N](parallelism: Int)(f: O => N) : Pipeline[I, N] = {
    addStep(parallelism) { o =>
      try Future.successful(f(o))
      catch { case t: Throwable => Future.failed(t) }
    }
  }

  private[this] def addStep[N](parallelism: Int)(f: O => Future[N]) : Pipeline[I, N] = {
    Stage(Func.fromValue(parallelism, f) :: stages, starter)
  }

  def future()(implicit system: ActorSystem) : FutureRunner[I, O] = new FutureRunner[I, O] {
    private[this] val (start, end) = runner(stages, () => new WaitEnd[O]())

    def apply(v: I) : Future[O] = {
      val promise = Promise[O]()
      start ! Value(v, v, promise)
      promise.future
    }

    def next() : Future[O] = {
      debug(s"FutureRunner: next()")
      val promise = Promise[O]()
      end ! promise
      promise.future
    }
  }

  def foreachM(parallelism: Int)(f: Try[O] => Unit)(implicit system: ActorSystem) : Runner[I, O] = new Runner[I, O] {

    private[this] val (start, _) = {

      val extra = Func(parallelism) { c =>
        f(c.toTry)
        c.toFuture
      }

      runner(extra :: stages, () => new CommitEnd)
    }

    def apply(v: I) = {
      val promise = Promise[O]()
      start ! Value(v, v, promise)
      promise.future
    }
  }

  private[this] def runner(stages: List[Func], endActor: () => Actor)(implicit system: ActorSystem) : (ActorRef, ActorRef) = {
    val creator = starter.apply[O](opts) _
    val start : ActorRef = system.actorOf(Props(creator = creator))

    val end : ActorRef = system.actorOf(Props(creator = endActor))

    def supervisor(func: Func, id: Int) : ActorRef = system.actorOf(Props(creator = () => Supervisor[func.In, func.Out](stages.size - id, func.f, func.p)))

    val sup = (supervisor _).tupled

    val actors : List[ActorRef] = (end :: stages.zipWithIndex.map(sup)).reverse
    start ! Bind(actors)
    (start, end)
  }

}
object Pipeline {
  val defaultOpts = Opts()

  def apply[I]()(implicit opts: Opts = defaultOpts) : Pipeline[I, I] = {

    new Stage(List(), new Starter[I] {
      def apply[Final](opts: Opts)(): Actor = Start[I]()(opts)
    })
  }

  def apply[O1](r1: FutureRunner[_, O1])(implicit opts: Opts = defaultOpts, exec: ExecutionContextExecutor) : Pipeline[O1, O1] = apply(1, r1)
  def apply[O1](parallelism: Int, r1: FutureRunner[_, O1])(implicit opts: Opts = defaultOpts, exec: ExecutionContextExecutor) : Pipeline[O1, O1] = {
    new Stage(List(), new Starter[O1] {
      def apply[Final](opts: Opts)(): Actor = JoinStart[O1, Final](r1.next)(opts)
    })
  }

  def apply[O1, O2](r1: FutureRunner[_, O1], r2: FutureRunner[_, O2])(implicit opts: Opts = defaultOpts, exec: ExecutionContextExecutor) : Pipeline[(O1, O2), (O1, O2)] = apply(1, r1, r2)
  def apply[O1, O2](parallelism: Int, r1: FutureRunner[_, O1], r2: FutureRunner[_, O2])(implicit opts: Opts = defaultOpts, exec: ExecutionContextExecutor) : Pipeline[(O1, O2), (O1, O2)] = {
    new Stage(List(), new Starter[(O1, O2)] {
      def apply[Final](opts: Opts)(): Actor = {
        def next() = {
          val f1 = r1.next
          val f2 = r2.next
          for {
            n1 <- f1
            n2 <- f2
          } yield (n1, n2)
        }
        JoinStart[(O1, O2), Final](next)(opts)
      }
    })
  }
}

private[pipeline] sealed trait Starter[In] {
  def apply[Final](opts: Opts)() : Actor
}

case class Opts(debug: Boolean = false)