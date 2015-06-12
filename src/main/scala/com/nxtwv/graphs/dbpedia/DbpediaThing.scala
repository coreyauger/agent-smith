package com.nxtwv.graphs.dbpedia

import java.net.URLEncoder

import com.nxtwv.graphs.common.Thing
import com.nxtwv.graphs.dbpedia.DbPediaThing.SchemaProperty
import play.api.libs.json.{JsArray, JsValue, Json}




object DbPediaThing{

  trait PropertyValue
  object PropertyValue{
    def apply(str:String):PropertyValue = {
      if(str.startsWith("{"))
        PropertyValueArray(str.substring(1, str.length - 1).split('|'))
      else if(str == "NULL")
        PropertyValueNull
      else
        PropertyValueString(str)
    }



    def toString(v:PropertyValue):String = {
      v match{
        case PropertyValueArray(list) => list.mkString("{","|","}")
        case PropertyValueString(str) => str
        case PropertyValueNull => "NULL"
      }
    }
  }

  case class SchemaProperty(propertyType: String, propertyTypeLabel: String, propertyName:String, propertyNameLabel:String,  propertyValue: PropertyValue, propertyValueLabel: Option[PropertyValue]) extends com.nxtwv.graphs.common.Thing.Property

  case object PropertyValueNull extends PropertyValue
  case class PropertyValueArray(list:Seq[String]) extends PropertyValue
  case class PropertyValueString(str:String) extends PropertyValue
  def cleanString(s:String) = s.replace("'","\'")
}

trait DbPediaBaseThing extends Thing{
  def uri:String
  def wikiPageID: String
  def wikiPageRevisionID: String
  def comment:String

  def entityUri = {
    //URLEncoder.encode(uri,"UTF-8")
    DbPediaThing.cleanString(uri)
  }
}


case class DbPediaThing(uri: String, label:String, wikiPageID:String, wikiPageRevisionID:String, comment:String, properties: Map[String,SchemaProperty]) extends DbPediaBaseThing
