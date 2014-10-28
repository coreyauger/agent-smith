package com.nxtwv.graphs.dbpedia

import com.nxtwv.graphs.common.Thing
import play.api.libs.json.{JsValue, Json}



object DbPediaThing{

  case class SchemaProperty(propertyType: String, propertyTypeLabel: String, propertyLabel: String, propertyURI: String) extends com.nxtwv.graphs.common.Thing.Property
  implicit val sf = Json.format[SchemaProperty]
  implicit val sr  = Json.reads[SchemaProperty]

}


class DbPediaThing(entityUri: String, label:String, properties: Map[String, Option[JsValue]]) extends Thing(label:String, properties: Map[String, Option[JsValue]]){
  // TODO: which items do we want to make sure are indexed?
}

class DbPediaVariant(entityUri: String, label:String, properties: Map[String, Option[JsValue]]) extends DbPediaThing(entityUri, label, properties)
