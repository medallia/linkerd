package io.buoyant.linkerd.protocol.http

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonSubTypes, JsonTypeInfo}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{ServiceFactory, Stack, Stackable}
import io.buoyant.router.RouterLabel
import io.buoyant.router.http.ApplyHostForwardedHeader

case class ApplyHostForwardedHeaderConfig() {

  /** Appends AddForwardedHeader params to the given params. */
  @JsonIgnore
  def ++:(params: Stack.Params): Stack.Params =
    params // + ApplyHostForwardedHeader.Enabled(true) 

}

/**
 * Translates ApplyHostForwardedHeaderConfig.Param into ApplyHostForwardedHeader
 * configuration parameters.
 */
object ApplyHostForwardedHeaderConfig {

  case class Param(config: Option[ApplyHostForwardedHeaderConfig])
  implicit object Param extends Stack.Param[Param] {
    val default = Param(None)
  }

  /**
   * Configures parameters for `ApplyHostForwardedHeader`.
   *
   *
   */
  val module: Stackable[ServiceFactory[Request, Response]] =
    new Stack.Module[ServiceFactory[Request, Response]] {
      val role = Stack.Role("ConfigureApplyHostForwardedHeader")
      val description = ApplyHostForwardedHeader.module.description
      val parameters = Seq(implicitly[Stack.Param[Param]])

      private type Stk = Stack[ServiceFactory[Request, Response]]

      def make(params: Stack.Params, stk: Stk) = {
        print("pepito")
        val mkNext: (Stack.Params, Stk) => Stk =
          (prms, next) => Stack.Leaf(this, next.make(prms))
        Stack.Node(this, mkNext, stk)
      }
      /*params[Param] match {
        case Param(None) => stk
        case Param(Some(config)) =>
          // Wrap the underlying stack, applying the ForwardedHeaderConfig
          val mkNext: (Stack.Params, Stk) => Stk =
            (prms, next) => Stack.Leaf(this, next.make(prms ++: config))
          Stack.Node(this, mkNext, stk)
      }*/
    }

}
