package co.torri.pipeline

import concurrent.Future

trait Runner[I, O] {
  def apply(v: I) : Future[O]
}
trait FutureRunner[I, O] extends Runner[I, O] {
  def apply(i: I) : Future[O]
  def next() : Future[O]
}