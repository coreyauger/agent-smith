package com.nxtwv.graphs.titan

import com.nxtwv.graphs.common.DataLoader
import com.nxtwv.graphs.dbpedia.DbPediaThing
import com.nxtwv.graphs.dbpedia.DbPediaThing.{PropertyValue, PropertyValueArray, PropertyValueNull, PropertyValueString}
import com.nxtwv.graphs.titan.io.surfkit.core.titan.TitanAPI

/**
  * Created by suroot on 12/06/15.
  */
object DbpediaTitanLoader extends DataLoader with TitanAPI {

   //Now that we know the property types.. we should do some of the following maps...
   // string -> simple string property
   // date -> makes to a date string and also entity gets a "Time" label
   // lat, lng -> gets the float values plus we add a "Location" label
   // When we know this is a "person" type maybe we add a person label (name, alias, birthdays, ect..)
     def toCypher(t:DbPediaThing):List[String] = {

         // create the entity
         s"""
           |MERGE (e:Entity
           |{
           | uri:'${t.entityUri}'
           |})
           |ON CREATE
           |SET
           |e.label = '${DbPediaThing.cleanString(t.label)}',
           |e.comment = '${DbPediaThing.cleanString(t.comment)}',
           |e.wikiPageID = ${t.wikiPageID},
           |e.wikiPageRevisionID = ${t.wikiPageRevisionID}
           |
         """.stripMargin  ::
         t.properties.values.zipWithIndex.map{
           case (p,i) =>
             p.propertyTypeLabel match{
               case "XMLSchema#string" if( !p.propertyValue.getClass.isInstance(PropertyValueNull)  ) =>
                 s"SET e.${p.propertyNameLabel} = '${PropertyValue.toString(p.propertyValue)}'\n"
               case "XMLSchema#int" | "XMLSchema#integer" | "XMLSchema#float" | "XMLSchema#decimal" | "XMLSchema#double" if( !p.propertyValue.getClass.isInstance(PropertyValueNull)  )  =>
                 if( p.propertyNameLabel.startsWith("wgs84_pos") ){ // lat / long (type float)
                   s"""
                      |SET e :Location
                      |SET e.${p.propertyNameLabel.substring(p.propertyNameLabel.indexOf('#')+1)} = ${PropertyValue.toString(p.propertyValue).replace("'","\'")}
                    """.stripMargin
                 }else {
                   p.propertyValue match {
                     case PropertyValueArray(arr) =>
                       // TODO: we just take the first value on a range for now..
                       //s"SET e.${p.propertyNameLabel} = ${arr.head}\n"
                       ""
                     case PropertyValueString(str) =>
                       s"SET e.${p.propertyNameLabel} = ${PropertyValue.toString(p.propertyValue)}\n"
                     case PropertyValueNull => ""
                   }
                 }
               case "XMLSchema#date" | "XMLSchema#gYear" if( !p.propertyValue.getClass.isInstance(PropertyValueNull)  )  =>
                 s"""
                    |SET e :Time
                    |SET e.${p.propertyNameLabel} = '${PropertyValue.toString(p.propertyValue)}'
                  """.stripMargin
               case "rdf-schema#Class" =>
                 p.propertyValue match{
                   case PropertyValueArray(arr) =>
                     val labelArray = p.propertyValueLabel.map{
                       case PropertyValueArray(a) => a
                       case _ => arr
                     }.getOrElse(arr)
                     arr.zip(labelArray).zipWithIndex.map{
                       //   |t${ii} :Entity  (CA) - took this out.. don't want types to also be entity when we do shortest path
                       case ((tUri,tLabel), ii) =>
                         s"""
                          |MERGE (t${ii}:Type {uri:'${DbPediaThing.cleanString(tUri)}'})
                          |CREATE UNIQUE (e)-[:${DbPediaThing.toRelationship(p.propertyNameLabel)} {uri:'${DbPediaThing.cleanString(p.propertyName)}'}]->(t${ii})
                        """.stripMargin
                     }.mkString("\n")
                   case PropertyValueString(str) =>
                     s"""
                          |MERGE (e${i}:Entity {uri:'${DbPediaThing.cleanString(str)}'})
                          |CREATE UNIQUE (e)-[:${DbPediaThing.toRelationship(p.propertyNameLabel)} {uri:'${DbPediaThing.cleanString(p.propertyName)}'}]->(e${i})
                        """.stripMargin
                   case PropertyValueNull => ""
                 }
               case _ =>
                 p.propertyValue match{
                   case PropertyValueArray(arr) =>
                     val labelArray = p.propertyValueLabel.map{
                       case PropertyValueArray(a) => a
                       case _ => arr
                     }.getOrElse(arr)
                     arr.zip(labelArray).zipWithIndex.map {
                       case ((eUri, eLabel), ii) =>
                         s"""
                          |MERGE (e${i}p${ii}:Entity {uri:'${DbPediaThing.cleanString(eUri)}'})
                          |CREATE UNIQUE (e)-[:${DbPediaThing.toRelationship(p.propertyNameLabel)} {uri:'${DbPediaThing.cleanString(p.propertyName)}'}]->(e${i}p${ii})
                        """.stripMargin
                     }.mkString("\n")
                   case PropertyValueString(str) =>
                     s"""
                      |MERGE (e${i}:Entity {uri:'${DbPediaThing.cleanString(str)}'})
                      |CREATE UNIQUE (e)-[:${DbPediaThing.toRelationship(p.propertyNameLabel)} {uri:'${DbPediaThing.cleanString(p.propertyName)}'}]->(e${i})
                    """.stripMargin
                   case PropertyValueNull => ""
                 }
             }
         }.mkString :: ";" :: Nil
     }
 }
