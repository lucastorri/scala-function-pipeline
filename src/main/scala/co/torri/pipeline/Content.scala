package co.torri.pipeline

import concurrent.{Future, Promise}
import util.{Try, Success, Failure}

sealed trait Content[+V] {
  def fail(t: Throwable) : Content[Nothing]
  def next[N](n: N) : Content[N]
  def commit() : Unit

  def toTry() : Try[V]
  def toFuture() : Future[V]

  def isEmpty() = false
  final def nonEmpty() = !isEmpty
}

case class Value[V, Original, Output](v: V, original: Original, output: Promise[Output]) extends Content[V] {

  override def fail(t: Throwable) : Content[Nothing] = Error(t, original, output)

  override def next[N](n: N) : Content[N] =  Value(n, original, output)

  def commit() = output.success(v.asInstanceOf[Output])

  def toTry() : Try[V] = Success(v)

  def toFuture() : Future[V] = Future.successful(v)
}

case class Error[Original, Output](t: Throwable, original: Original, output: Promise[Output]) extends Content[Nothing] {

  override def fail(t: Throwable) : Content[Nothing] = this

  override def next[N](n: N) : Content[N] = this

  def commit() = output.failure(t)

  def toTry() : Try[Nothing] = Failure(t)

  def toFuture() : Future[Nothing] = Future.failed(t)
}

object NoContent extends Error(new Exception("NoContent"), (), Promise()) {
  override def isEmpty() = true
}