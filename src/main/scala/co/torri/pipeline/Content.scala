package co.torri.pipeline

import concurrent.Promise

sealed trait Content[+V] {
  def fail(t: Throwable) : Content[Nothing]
  def next[N](n: N) : Content[N]
}

case class Value[V, Original, Output](v: V, original: Original, output: Promise[Output]) extends Content[V] {

  override def fail(t: Throwable) : Content[Nothing] = Error(t, original, output)

  override def next[N](n: N) : Content[N] =  Value(n, original, output)
}

case class Error[Original, Output](t: Throwable, original: Original, output: Promise[Output]) extends Content[Nothing] {

  override def fail(t: Throwable) : Content[Nothing] = this

  override def next[N](n: N) : Content[N] = this
}
