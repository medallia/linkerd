package io.buoyant

import com.twitter.util.{Await, Duration}
import io.buoyant.admin.{AdminInitializer, App}
import io.buoyant.linkerd.Linker.LinkerConfig
import io.buoyant.linkerd.admin.LinkerdAdmin
import io.buoyant.linkerd.{Build, Linker}
import java.io.File

import sun.misc.Signal
import sun.misc.SignalHandler

import scala.io.Source

/**
 * linkerd main execution.
 *
 * usage: linkerd path/to/config
 *
 * The config file may be either JSON- or YAML-formatted
 */
object Linkerd extends App {

  protected[this] val graceFlag = flag("l5d.grace", 10, "Grace shutdown time (secs)")

  val shutdownHandler: SignalHandler = new SignalHandler() {
    def handle(sig: Signal): Unit = {
      log.info("Closing all ...")
      Await.result(close(Duration.fromSeconds(graceFlag())))
      log.info("All closed.")
    }
  }

  def main() {
    Signal.handle(new Signal("INT"), shutdownHandler)
    Signal.handle(new Signal("TERM"), shutdownHandler)

    val build = Build.load(getClass.getResourceAsStream("/io/buoyant/linkerd-main/build.properties"))
    log.info("linkerd %s (rev=%s) built at %s", build.version, build.revision, build.name)

    args match {
      case Array(path) =>

        val linkerConfig = loadLinker(path)
        val linker = linkerConfig.mk

        val linkerdAdmin = new LinkerdAdmin(this, linker, linkerConfig)
        val adminInitializer = new AdminInitializer(linker.admin, linkerdAdmin.adminMuxer)
        adminInitializer.startServer()
        closeOnExit(adminInitializer.adminHttpServer)

        // TODO initialize:
        // - namers
        // - tracers

        val telemeters = linker.telemeters.map(_.run())
        telemeters.foreach(closeOnExit(_))

        val routers = linker.routers.flatMap { router =>
          val running = router.initialize()
          closeOnExit(running)
          running.servers.map { server =>
            log.info("serving %s on %s:%d", server.router, server.ip, server.port)
            val listening = server.serve()
            for (name <- server.announce) {
              val announcers = running.announcers.filter {
                case (prefix, announcer) => name.startsWith(prefix)
              }
              for ((prefix, announcer) <- announcers) {
                log.info("announcing %s as %s to %s", server.addr, name.show, announcer.scheme)
                announcer.announce(server.addr, name.drop(prefix.size)).onSuccess(closeOnExit)
              }
              if (announcers.isEmpty) log.warning("no announcer found for %s", name.show)
            }
            closeOnExit(listening)
            listening
          }
        }
        log.info("linkerd initialized.")
        Await.all(routers ++ telemeters: _*)

      case _ => exitOnError("usage: linkerd path/to/config")
    }
  }

  private def loadLinker(path: String): LinkerConfig = {
    val configText = path match {
      case "-" =>
        Source.fromInputStream(System.in).mkString
      case path =>
        val f = new File(path)
        if (!f.isFile) throw new IllegalArgumentException(s"config is not a file: $path")
        Source.fromFile(f).mkString
    }

    Linker.parse(configText)
  }

}
