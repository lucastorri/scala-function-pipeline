package co.torri.pipeline

import co.torri.pipeline.actors._
import concurrent.Promise
import akka.actor.{Props, ActorRef}

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