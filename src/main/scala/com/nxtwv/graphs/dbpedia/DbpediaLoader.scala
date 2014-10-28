package com.nxtwv.graphs.dbpedia

import com.nxtwv.graphs.common.{DataLoader, NeoService}
import com.nxtwv.graphs.dbpedia.DbPediaThing.SchemaProperty
import scala.concurrent.ExecutionContext.Implicits.global
import play.api.libs.json._

/**
 * Created by suroot on 23/10/14.
 */
class DbpediaLoader extends DataLoader with NeoService {

  implicit def neo4jserver = new Neo4JServer("127.0.0.1", 7474, "/db/data/")
  // make sure we have setup the unique constraints
  println("contraints")
  Q("CREATE CONSTRAINT ON (t:Type) ASSERT t.typeUri IS UNIQUE;").getOneJs
  Q("CREATE CONSTRAINT ON (e:Entity) ASSERT e.entityUri IS UNIQUE;").getOneJs


  private def convertToPropsList(js:JsValue, s: SchemaProperty):(String, Option[JsValue]) = {
    val t = (js \\ s.propertyURI)
    t match{
      case list :: Nil =>
        //println(s"${s.propertyLabel} = ${list}")
        (s.propertyLabel -> Some(list))
      case Nil =>
        (s.propertyLabel -> None)
    }
  }

  private def mergeTypeHeirarchy(types:List[String]) = {
    types.map{ t =>
    s"""
      |MERGE (t:Type {typeUri:'$t'});
    """.stripMargin
    }
  }

  def load(json:JsValue) = {

    val props = (json \ "properties").as[List[SchemaProperty]]
    val instances = (json \ "instances").as[List[JsValue]]
    val entities = instances.map{
      case js:JsValue =>
        // rdf-schema#label
        // 22-rdf-syntax-ns#type
        val map = props.map{
          case s:SchemaProperty =>
            convertToPropsList(js,s)
        }.filter( _ != None ).toMap

        val label = (js \\ "http://www.w3.org/2000/01/rdf-schema#label").head.toString

        if( label.contains("NULL") ){
          (js \\ "http://dbpedia.org/ontology/variantOf_label") match{
            case x :: xs  =>
              new DbPediaVariant( js.asInstanceOf[JsObject].keys.head, (js \\ "http://dbpedia.org/ontology/variantOf_label").head.toString, map  )
            case _ =>
              new DbPediaThing( js.asInstanceOf[JsObject].keys.head, null, map  )
          }
        }else{
          println(s" .. '$label'")
          new DbPediaThing( js.asInstanceOf[JsObject].keys.head, (js \\ "http://www.w3.org/2000/01/rdf-schema#label").head.toString, map  )
        }
    }
    // break into groups of 100 items per group...
    val groups = entities.grouped(1000).toList
    println(s"BATCHING into ${groups.length}")
    groups.foreach {
      list =>
        list.map {
          case v: DbPediaVariant =>
            println("variant")
          case t: DbPediaThing =>
            val types = mergeTypeHeirarchy(t.properties.getOrElse("22-rdf-syntax-ns#type_label", Some(Json.arr(""))).get.as[List[String]])
            batchCypher(types).map{
              case js =>
                println(js)
            }
          case _ =>
            println("Unknown Entity type")
        }



    }

  }
}
