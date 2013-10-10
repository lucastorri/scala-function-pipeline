package co.torri

import akka.actor.ActorSystem

package object pipeline {

  private[pipeline] val system = ActorSystem()

  def debug(a: Any) = {}//println(a)

}
