agent-smith
===========
### What is it?
Scala Spark dbpedia loader for both Neo4J and Titan.  

### Gaols
* Raad in csv dbpedia dump and extract features from the file
* Convert features into case class that we can use to load data into other formats.
* Load data into a Neo4J database
* Load data into a Titan database.

### Overview

For my purposes it was sufficiant to convert the data into the following structure

```scala
case class DbPediaThing(uri: String, label:String, wikiPageID:String, wikiPageRevisionID:String, comment:String, properties: Map[String,SchemaProperty]) extends DbPediaBaseThing

case class SchemaProperty(propertyName:String, propertyNameLabel:String,  propertyType: String, propertyTypeLabel: String, propertyValue: PropertyValue, propertyValueLabel: Option[PropertyValue]) extends com.nxtwv.graphs.common.Thing.Property
```

From here we can load the data using the Neo4J loader or the Titan loader.
### Neo4j
Note the ugly Await :(
Simply doing your work async will overflow the connector and lead to errors and failures to commit.  There are obviously better ways to do this then Await.. Which I may add in future
```scala
val cypher = things.map(a => DbpediaCypherLoader.toCypher(a).mkString("\n"))
    cypher.zipWithIndex().map{ case (s,i) => (i % factor,s) }.groupByKey.map{
      case (k, xs) =>
        println(s"working batch: $k of $factor")
        Await.ready(DbpediaCypherLoader.batchCypher(xs.toList), 30.minutes)
        "."
    }

```

### Titan
Again we use a blocking connection that makes sure we don't hammer the connector.  This needs to be fixed :(
```scala
val gremlin = things.map(a => DbpediaTitanLoader.toGremlin(a).mkString("\n") )
    gremlin.zipWithIndex.foreach{ case (gg, ind) =>
      println((ind.toDouble/count.toDouble)*100.0)
      gg.split("\n\n").foreach { g =>
        val rs = DbpediaTitanLoader.execGremlin(g)
        print(".")
      }
    }
```

### Product
![http://www.coreyauger.com/images/agent-smith.png](http://www.coreyauger.com/images/agent-smith.png)
