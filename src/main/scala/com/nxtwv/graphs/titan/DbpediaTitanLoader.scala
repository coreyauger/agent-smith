package com.nxtwv.graphs.titan

import com.nxtwv.graphs.common.DataLoader
import com.nxtwv.graphs.dbpedia.DbPediaThing
import com.nxtwv.graphs.dbpedia.DbPediaThing.{PropertyValue, PropertyValueArray, PropertyValueNull, PropertyValueString}
import com.nxtwv.graphs.titan.io.surfkit.core.titan.TitanAPI

/**
  * Created by suroot on 12/06/15.
  */
object DbpediaTitanLoader extends DataLoader with TitanAPI {

  def createUnique(e1Uri:String, rel:String, e2Uri:String):String = {
    s"""
       |try{
       |  if(! g.V().has(label,'entity').has('uri','${e1Uri}').hasNext() ){
       |  graph.addVertex(label, 'entity','uri','${e1Uri}');
       |  graph.tx().commit();
       |}}catch(e){};
       |try{
       |  if(! g.V().has(label,'entity').has('uri','${e2Uri}').hasNext() ){
       |  graph.addVertex(label, 'entity','uri','${e2Uri}');
       |  graph.tx().commit();
       |}}catch(e){};
       |try{
       |e1 = g.V().has(label,'entity').has('uri','${e1Uri}').next();
       |e2 = g.V().has(label,'entity').has('uri','${e2Uri}').next();
       |if(! g.V().has(label,'entity').has('uri','${e1Uri}').out('${rel}').has('uri','${e2Uri}').hasNext() ){
       |  e1.addEdge('${rel}',e2);
       |  graph.tx().commit();
       |}
       |}catch(e){};
     """.stripMargin
  }

   //Now that we know the property types.. we should do some of the following maps...
   // string -> simple string property
   // date -> makes to a date string and also entity gets a "Time" label
   // lat, lng -> gets the float values plus we add a "Location" label
   // When we know this is a "person" type maybe we add a person label (name, alias, birthdays, ect..)
     def toGremlin(t:DbPediaThing):List[String] = {

         // create the entity
         s"""
           |try{
           |if(! g.V().has(label,'entity').has('uri','${t.entityUri}').hasNext() ){
           |  graph.addVertex(label, 'entity','uri','${t.entityUri}','wikiLabel','${DbPediaThing.cleanString(t.label)}','comment','${DbPediaThing.cleanString(t.comment)}','wikiPageID',${t.wikiPageID},'wikiPageRevisionID',${t.wikiPageRevisionID});
           |  graph.tx().commit();
           |}
           |}catch(e){};
           |
         """.stripMargin  ::
         t.properties.values.zipWithIndex.map{
           case (p,i) =>
             p.propertyTypeLabel match{
               case "XMLSchema#string" if( !p.propertyValue.getClass.isInstance(PropertyValueNull)  ) =>
                 //s"SET e.${p.propertyNameLabel} = '${PropertyValue.toString(p.propertyValue)}'\n"
                 s"try{ g.V().has(label,'entity').has('uri','${t.entityUri}').next().property('${p.propertyNameLabel}','${PropertyValue.toString(p.propertyValue)}'); }catch(e){};"
               case "XMLSchema#int" | "XMLSchema#integer" | "XMLSchema#float" | "XMLSchema#decimal" | "XMLSchema#double" if( !p.propertyValue.getClass.isInstance(PropertyValueNull)  )  =>
                 if( p.propertyNameLabel.startsWith("wgs84_pos") ){ // lat / long (type float)
                   // TODO: add label "location"
                   s"try{ g.V().has(label,'entity').has('uri','${t.entityUri}').next().property('${p.propertyNameLabel.indexOf('#')+1}','${PropertyValue.toString(p.propertyValue).replace("'","\'")}'); }catch(e){};"
                 }else {
                   p.propertyValue match {
                     case PropertyValueArray(arr) =>
                       // TODO: we just take the first value on a range for now..
                       //s"SET e.${p.propertyNameLabel} = ${arr.head}\n"
                       ""
                     case PropertyValueString(str) =>
                       //s"SET e.${p.propertyNameLabel} = ${PropertyValue.toString(p.propertyValue)}\n"
                       s"try{ g.V().has(label,'entity').has('uri','${t.entityUri}').next().property('${p.propertyNameLabel}',${PropertyValue.toString(p.propertyValue)});}catch(e){};"
                     case PropertyValueNull => ""
                   }
                 }
               case "XMLSchema#date" | "XMLSchema#gYear" if( !p.propertyValue.getClass.isInstance(PropertyValueNull)  )  =>
                 // TODO: add label "time"
                 s"try{ g.V().has(label,'entity').has('uri','${t.entityUri}').next().property('${p.propertyNameLabel}','${PropertyValue.toString(p.propertyValue)}');}catch(e){};"
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
                         createUnique(t.entityUri, DbPediaThing.toRelationship(p.propertyNameLabel), DbPediaThing.cleanString(tUri))
                     }.mkString("\n")
                   case PropertyValueString(str) =>
                     createUnique(t.entityUri, DbPediaThing.toRelationship(p.propertyNameLabel), DbPediaThing.cleanString(str))
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
                         createUnique(t.entityUri, DbPediaThing.toRelationship(p.propertyNameLabel), DbPediaThing.cleanString(eUri))
                     }.mkString("\n")
                   case PropertyValueString(str) =>
                     createUnique(t.entityUri, DbPediaThing.toRelationship(p.propertyNameLabel), DbPediaThing.cleanString(str))
                   case PropertyValueNull => ""
                 }
             }
         }.mkString :: ";" :: Nil
     }
 }
