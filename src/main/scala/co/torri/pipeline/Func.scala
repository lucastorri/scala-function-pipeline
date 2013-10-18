package co.torri.pipeline

import concurrent.Future

trait Func { self =>
  type In
  type Out

  def p : Int
  def f : Content[In] => Future[Out]
}
object Func {
  def fromValue[I, O](parallelism: Int, ff: I => Future[O]) : Func = {
    def f(t: Content[I]) : Future[O] = t match {
      case Value(v, _, _) => ff(v)
      case Error(e, _, _) => Future.failed(e)
    }
    apply(parallelism)(f)
  }

  def apply[I, O](parallelism: Int)(ff: Content[I] => Future[O]) : Func = new Func {
    type In = I
    type Out = O

    val p = parallelism
    val f = ff
  }
}
