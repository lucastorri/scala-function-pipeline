package co.torri.pipeline

import concurrent.Future

trait Runner[I, O]
trait CallbackRunner[I, O] extends Runner[I, O] {
  def apply(v: I)
}
trait FutureRunner[I, O] extends Runner[I, O] {
  def apply(i: I) : Future[O]
}
trait NextRunner[I, O] extends Runner[I, O] {
  def apply(i: I)
  def next() : Future[O]
}