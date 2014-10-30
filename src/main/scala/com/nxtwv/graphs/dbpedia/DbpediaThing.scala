package com.nxtwv.graphs.dbpedia

import java.net.URLEncoder

import com.nxtwv.graphs.common.Thing
import play.api.libs.json.{JsArray, JsValue, Json}




object DbPediaThing{

  case class SchemaProperty(propertyType: String, propertyTypeLabel: String, propertyLabel: String, propertyURI: String) extends com.nxtwv.graphs.common.Thing.Property
  implicit val sf = Json.format[SchemaProperty]
  implicit val sr  = Json.reads[SchemaProperty]

}


class DbPediaThing(private val uri: String, label:String, properties: Map[String, Option[JsValue]]) extends Thing(label:String, properties: Map[String, Option[JsValue]]){

  override def toString = {
    val str = (s"UIR :: $entityUri\n") + super.toString
    str.mkString
  }

  def entityUri = {
    URLEncoder.encode(uri,"UTF-8")
  }

  // TODO: which items do we want to make sure are indexed?
  def toJson ={
    val map = Map(
      "uri" -> entityUri,
      "label" -> label.replace("'","\\'")
    )
    /*
    ++ properties.filter{ case (k,v) => v != None }.map{
      case (k,v) =>
        v match{
          case Some(xs:JsArray) =>
            println(xs)
            (k, xs.as[List[String]].mkString(","))
          case Some(x:JsValue) =>
            println(x)
            (k, x.as[String])
        }

    }
    */
    Json.toJson(map)
  }
}

class DbPediaVariant(entityUri: String, label:String, properties: Map[String, Option[JsValue]]) extends DbPediaThing(entityUri, label, properties)
