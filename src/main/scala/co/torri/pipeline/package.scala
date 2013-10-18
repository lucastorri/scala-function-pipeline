package co.torri

import akka.actor.ActorSystem

package object pipeline {

  private[pipeline] implicit val system = ActorSystem()

  def debug(a: => Any)(implicit opts: Opts) = if (opts.debug) println(a)

}
