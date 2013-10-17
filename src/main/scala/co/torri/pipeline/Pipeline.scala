package co.torri.pipeline

import co.torri.pipeline.actors._
import concurrent.{ExecutionContextExecutor, Future, Promise}
import akka.actor.{ActorSystem, Props, ActorRef}

trait Pipeline[I, O] {
  def fork[N1](parallelism: Int, p1: Pipeline[O, N1])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1)]
  def fork[N1](p1: Pipeline[O, N1])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1)] = fork(1, p1)

  def fork[N1, N2](parallelism: Int, p1: Pipeline[O, N1], p2: Pipeline[O, N2])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1, N2)]
  def fork[N1, N2](p1: Pipeline[O, N1], p2: Pipeline[O, N2])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1, N2)] = fork(1, p1, p2)

  def mapM[N](parallelism: Int)(f: O => N) : Pipeline[I, N]
  def map[N](f: O => N) : Pipeline[I, N] = mapM(1)(f)

  def pipe()(implicit system: ActorSystem) : FutureRunner[I, O]
  def pipe(o: Output[O])(implicit system: ActorSystem) : CallbackRunner[I, O]
}
case class Stage[I, O] private[pipeline](stages: List[Func]) extends Pipeline[I, O] {

  def fork[N1](parallelism: Int, p1: Pipeline[O, N1])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1)] = {

    val pipe1 = p1.pipe

    addStep(parallelism) { o =>
      for {
        o1 <- pipe1(o)
      } yield (o, o1)
    }
  }
  def fork[N1, N2](parallelism: Int, p1: Pipeline[O, N1], p2: Pipeline[O, N2])(implicit exec: ExecutionContextExecutor) : Pipeline[I, (O, N1, N2)] = {

    val pipe1 = p1.pipe
    val pipe2 = p2.pipe

    addStep(parallelism) { o =>
      for {
        o1 <- pipe1(o)
        o2 <- pipe2(o)
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
    Stage(Func(parallelism, f) :: stages)
  }

  def pipe()(implicit system: ActorSystem) : FutureRunner[I, O] = new FutureRunner[I, O] {
    private[this] val start = runner(Output.noop[O])(system)

    def apply(v: I) = {
      val promise = Promise[O]()
      start ! Value(v, v, promise)
      promise.future
    }
  }

  def pipe(output: Output[O])(implicit system: ActorSystem) : CallbackRunner[I, O] = new CallbackRunner[I, O] {
    private[this] val start = runner(output)(system)

    def apply(v: I) = {
      val promise = Promise[O]()
      start ! Value(v, v, promise)
    }
  }

  private[this] def runner(output: Output[O])(system: ActorSystem) : ActorRef = {
    val start : ActorRef = system.actorOf(Props(creator = () => new Start {
      type In = I
    }))

    val end : ActorRef = system.actorOf(Props(creator = () => new End {
      type Out = O
    }))

    def supervisor(func: Func) : ActorRef = system.actorOf(Props(creator = () => new Supervisor {
      def parallelism: Int = func.p

      def f = func.f

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