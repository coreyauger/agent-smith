package com.nxtwv.graphs.common

import com.nxtwv.graphs.dbpedia.DbPediaThing.SchemaProperty
import play.api.libs.json.{JsValue, Json}


object Thing {
  trait Property
}


trait Thing{
  def label:String
  def properties:Map[String, SchemaProperty]

  override def toString = {
    val str = (s"TYPE :: $label") ::
      properties.map{
        case (prop, value) =>
          s"$prop = $value"
      }.toList
    str.mkString
  }
}
