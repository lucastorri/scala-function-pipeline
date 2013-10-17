package co.torri.pipeline

trait Output[O] {
  def apply(v: O, continue: () => Unit)
  def error(t: Throwable, continue: () => Unit)
}
trait AutoOutput[O] extends Output[O] {
  def apply(v: O)
  def error(t: Throwable)

  final def apply(v: O, continue: () => Unit) = {
    apply(v)
    continue()
  }
  final def error(t: Throwable, continue: () => Unit) = {
    error(t)
    continue()
  }
}
object Output {
  def noop[O] : Output[O] = new AutoOutput[O]() {
    def error(t: Throwable) = {}
    def apply(v: O) = {}
  }
  def success[O](f: O => Unit) : Output[O] = new AutoOutput[O] {
    def error(t: Throwable) = {}
    def apply(v: O) = f(v)
  }
}
