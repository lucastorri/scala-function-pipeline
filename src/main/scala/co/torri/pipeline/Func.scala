package co.torri.pipeline

import concurrent.Future

trait Func {
  type In
  type Out

  def p : Int
  def f : In => Future[Out]
}
object Func {
  def apply[I, O](parallelism: Int, ff: I => Future[O]) : Func = new Func {
    type In = I
    type Out = O

    val p = parallelism
    val f = ff
  }
}
