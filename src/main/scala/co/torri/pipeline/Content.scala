package co.torri.pipeline

import concurrent.Promise

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
