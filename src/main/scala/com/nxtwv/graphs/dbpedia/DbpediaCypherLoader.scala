package com.nxtwv.graphs.dbpedia

import com.nxtwv.graphs.common.{DataLoader, NeoService}

/**
 * Created by suroot on 12/06/15.
 */
class DbpediaCypherLoader extends DataLoader with NeoService {

    implicit def neo4jserver = new Neo4JServer("127.0.0.1", 7474, "/db/data/")
    // make sure we have setup the unique constraints
    println("contraints")
    Q("CREATE CONSTRAINT ON (t:Type) ASSERT t.uri IS UNIQUE;").getOneJs
    Q("CREATE CONSTRAINT ON (e:Entity) ASSERT e.uri IS UNIQUE;").getOneJs

  /*
    def toCypher(list:List[DbPediaThing]):List[String] = {
      list.flatMap{
        t =>
          // create the entity
          s"""
            |MERGE (e:Entity
            |{
            | uri:'${t.entityUri}'
            |})
            |ON CREATE
            |SET
            |e.label:'${DbPediaThing.cleanString(t.label)}'
            |e.comment: '${DbPediaThing.cleanString(t.comment)}',
            |e.wikiPageID: ${t.wikiPageID},
            |e.wikiPageRevisionID: ${t.wikiPageRevisionID}
            |
          """.stripMargin  ++
          t.properties.values.zipWithIndex.map{
            case (p,i) =>
              // create the TYPE first
              s"""
                |
                |MERGE (t${i}:Type
                |{
                | uri:'${p.propertyType}'
                |}) ON CREATE SET t${i}.label = '${p.propertyTypeLabel}'
                |CREATE UNIQUE (e)-[:
              """.stripMargin
          }
      }
    }
    */

}
