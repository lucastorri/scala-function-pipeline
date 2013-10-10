package co.torri.pipeline

trait Output[O] {
  def apply(v: O)
  def error(e: Exception)
}
object Output {
  def noop[O] = new Output[O]() {
    def apply(v: O) = {}
    def error(e: Exception) = {}
  }
}
