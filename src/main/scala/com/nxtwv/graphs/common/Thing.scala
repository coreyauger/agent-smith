package com.nxtwv.graphs.common

import play.api.libs.json.{JsValue, Json}


object Thing {
  trait Property

}


class Thing(val label:String, val properties: Map[String, Option[JsValue]]){
  override def toString = {
    val str = (s"TYPE :: $label") ::
      properties.map{
        case (prop, value) =>
          s"$prop = $value"
      }.toList
    str.mkString
  }
}
