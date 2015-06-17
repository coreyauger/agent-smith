package com.nxtwv.graphs

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.nxtwv.graphs.dbpedia.DbPediaThing
import com.nxtwv.graphs.dbpedia.DbPediaThing._
import com.nxtwv.graphs.dbpedia.DbpediaCypherLoader
import scala.concurrent.Future
import scala.Predef._
import scala.concurrent.Await
import scala.concurrent.duration._
/**
 *
 * Created by suroot on 14/10/14.
 */

object Main extends App{

  override def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Agent Smith").setMaster("local[4]").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)
    // Read the CSV file
    //val csv = sc.textFile("/home/suroot/projects/data/dbpedia/csv/Magazine.csv.gz")
    //val csv = sc.textFile("/home/suroot/projects/data/dbpedia/csv/ComicsCharacter.csv.gz")
    //val csv = sc.textFile("/home/suroot/projects/data/dbpedia/csv/Film.csv.gz")
    //val csv = sc.textFile("/home/suroot/projects/data/dbpedia/csv/Album.csv.gz")
    val csv = sc.textFile("/home/suroot/projects/data/dbpedia/csv/Actor.csv.gz")
    // split / clean data
    val headerAndRows = csv.map(line => line.split("\",\"").map(_.trim.replace("\"","")))
    // get header

    //val header = headerAndRows.first
    val headerRdd = headerAndRows.take(4)
    val header = headerRdd(0).zip(headerRdd(1)).zip(headerRdd(2)).zip(headerRdd(3)).map{
      case (((a,b),c),d) =>
        a -> SchemaProperty(b, a, d, c, PropertyValue(b), Some(PropertyValue(a)))
    }.toMap
    val keys = headerRdd(0)
    //val data = headerAndRows.filter(_(0) != header(0))
    val data = headerAndRows.zipWithIndex().filter(_._2 >= 4L).map(_._1)
    // splits to map (header/value pairs)
    val instances = data.map(splits =>keys.zip(splits).toMap)

//    header.foreach(println)

    // print result
    //instances.take(1).foreach(println)
    //instances.foreach(m => println(m.get("URI")))


    val filterKeySet = Set("URI","rdf-schema#label","wikiPageID","wikiPageRevisionID","rdf-schema#comment")
    val things = instances.map{
      m =>
        DbPediaThing(m("URI"), m("rdf-schema#label"), m("wikiPageID"), m("wikiPageRevisionID"), m("rdf-schema#comment"), m.filterKeys( k => !k.contains("_label") && !filterKeySet.contains(k)).map{
          prop =>
            prop._1 -> SchemaProperty(header(prop._1).propertyName, header(prop._1).propertyNameLabel, header(prop._1).propertyType, header(prop._1).propertyTypeLabel, PropertyValue(prop._2), m.get(s"${prop._1}_label").map(PropertyValue(_)) )
        })
    }
    things.cache()
    val count = things.count()
    println(s"Num records ${count}" )

    val factor = count / 250


    val cypher = things.map(a => DbpediaCypherLoader.toCypher(a).mkString("\n"))
    cypher.zipWithIndex().map{ case (s,i) => (i % factor,s) }.groupByKey.map{
    //cypher.map{
      case (k, xs) =>
    //  case xs =>
        println(s"working batch: $k of $factor")
       // Await.ready(DbpediaCypherLoader.execute(xs), 2.minutes)
        Await.ready(DbpediaCypherLoader.batchCypher(xs.toList), 30.minutes)
        "."
    }.foreach(print)

    // Now that we know the property types.. we should do some of the following maps in neo...
    // string -> simple string property
    // date -> makes to a date string and also entity gets a "Time" label
    // lat, lng -> gets the float values plus we add a "Location" label
    // When we know this is a "person" type maybe we add a person label (name, alias, birthdays, ect..)

    //instances.filter(_("rdf-schema#label").contains("WOD")).foreach(m => println(m.get("URI")))
    //cypher.take(2).foreach(println)
    println(s"Num records ${count}" )
    sc.stop()

  }



}
