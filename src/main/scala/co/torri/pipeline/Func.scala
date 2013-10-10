package co.torri.pipeline

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
