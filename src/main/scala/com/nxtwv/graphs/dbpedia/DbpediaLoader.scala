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
  Q("CREATE CONSTRAINT ON (t:Type) ASSERT t.uri IS UNIQUE;").getOneJs
  Q("CREATE CONSTRAINT ON (e:Entity) ASSERT e.uri IS UNIQUE;").getOneJs


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

  private def mergeTypeHierarchy(types:List[String], entity: DbPediaThing):List[String] = {
    types.map{ t =>
      (s"""
      |MERGE (t:Type {uri:'$t'});
      """.stripMargin,
      s"""
      |MATCH (t:Type {uri:'$t'}), (e:Entity {uri:'${entity.entityUri}'}) CREATE UNIQUE (e)-[:INSTANCE_OF]->(t);
      """.stripMargin
      )
    }.unzip match{
      case (as, bs) =>
        as ::: bs
    }
  }

  private def relator(thing:DbPediaThing, ontologies:List[SchemaProperty]) = {
    ontologies.map{
      case s:SchemaProperty =>
        val theType = List(s.propertyType)  // Type of what we are pointing to
        // s.propertyLabel   // the label for the edge .. eg: [CREATOR]

        // Need to make sure that the type exists..
        // Create the entity and map it to it's type
        // Relate this entity to the new entity with the above property label..

        //thing.properties.getOrElse(s.propertyLabel,"").get.as[List[String]].map{
        //  prop =>
        //    println(prop)
        //}

        //val thing = new DbPediaThing(s.propertyURI)
        println(thing)
        println(s)
        println()
        throw new Exception("")
    }
  }

  def load(json:JsValue) = {

    val props = (json \ "properties").as[List[SchemaProperty]]
    val ontologies = props.filter( _.propertyType.startsWith("http://dbpedia.org/ontology") )
    val instances = (json \ "instances").as[List[JsValue]]
    val entities = instances.map{
      case js:JsValue =>
        //println(Json.prettyPrint(js))
        //throw new Exception("")
        // rdf-schema#label
        // 22-rdf-syntax-ns#type
        val map = props.map{
          case s:SchemaProperty =>
            convertToPropsList(js,s)
        }.filter( _ != None ).toMap
        val label = (js \\ "http://www.w3.org/2000/01/rdf-schema#label").head.as[String]
        if( label.contains("NULL") ){
          (js \\ "http://dbpedia.org/ontology/variantOf_label") match{
            case x :: xs  =>
              new DbPediaVariant( js.asInstanceOf[JsObject].keys.head, (js \\ "http://dbpedia.org/ontology/variantOf_label").head.as[String], map  )
            case _ =>
              new DbPediaThing( js.asInstanceOf[JsObject].keys.head, null, map  )
          }
        }else{
          new DbPediaThing( js.asInstanceOf[JsObject].keys.head, (js \\ "http://www.w3.org/2000/01/rdf-schema#label").head.as[String], map  )
        }
    }
    // break into groups of 100 items per group...
    val groups = entities.grouped(1000).toList
    println(s"BATCHING into ${groups.length}")
    groups.foreach {
      list =>
        val statments = list.map {
          case v: DbPediaVariant =>
            println("variant")
              s"""
                 |MERGE (e:Entity {uri:'${v.entityUri}',label:'${v.label.replace("'","\\'")}'});
              """.stripMargin :: mergeTypeHierarchy(v.properties.getOrElse("22-rdf-syntax-ns#type_label", Some(Json.arr(""))).get.as[List[String]], v)
          case t: DbPediaThing =>
            relator(t,ontologies)
              s"""
                |MERGE (e:Entity {uri:'${t.entityUri}',label:'${t.label.replace("'","\\'")}'});
              """.stripMargin :: mergeTypeHierarchy(t.properties.getOrElse("22-rdf-syntax-ns#type_label", Some(Json.arr(""))).get.as[List[String]], t)
          case _ =>
            println("Unknown Entity type")
            List("")
        }
        //println(statments.flatten.toList)
        batchCypher(statments.flatten.toList).map{
          case js =>
            println(js)
        }
    }

  }
}
