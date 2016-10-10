package io.buoyant.namer.curatorsd

import com.fasterxml.jackson.databind.JsonMappingException
import com.medallia.l5d.curatorsd.{CuratorConfig, CuratorInitializer}
import com.twitter.finagle.util.LoadService
import io.buoyant.config.Parser
import io.buoyant.namer.{NamerConfig, NamerInitializer}
import io.buoyant.test.Exceptions
import org.scalatest.FunSuite

class CuratorTest extends FunSuite with Exceptions {

  def parse(yaml: String): CuratorConfig = {
    val mapper = Parser.objectMapper(yaml, Iterable(Seq(CuratorInitializer)))
    mapper.readValue[NamerConfig](yaml).asInstanceOf[CuratorConfig]
  }

  test("zkConnectStr list") {
    val yaml = """
kind: io.l5d.curator
zkConnectStr: foo:2181,bar:2181/chroot
"""
    assert(parse(yaml).zkConnectStr == "foo:2181,bar:2181/chroot")
  }

  test("missing hostname") {
    val yaml = """
kind: io.l5d.curator
"""
    assertThrows[JsonMappingException](parse(yaml))
  }

  test("service registration") {
    assert(LoadService[NamerInitializer]().exists(_.isInstanceOf[CuratorInitializer]))
  }
}
