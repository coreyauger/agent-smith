package com.nxtwv.graphs.titan

package io.surfkit.core.titan

import java.io.PrintWriter
import java.util.UUID
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.ResultSet

import scala.collection.JavaConversions._
import java.util.function.{Consumer => JConsumer}
import scala.compat.java8.FutureConverters._
import org.apache.tinkerpop.gremlin.driver.{ResultSet, Cluster}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

/**
 * Created by suroot on 18/11/15.
 */
trait TitanAPI {
  //usage example: `i: Int ⇒ 42`
  implicit def toJavaConsumer[A, B](f: Function1[A, Unit]) = new JConsumer[A] {
    override def accept(a: A): Unit = f(a)
  }


  import scala.io.Source

  // The string argument given to getResource is a path relative to
  // the resources directory.
  val remoteYaml = Source.fromURL(getClass.getResource("/remote.yaml"))
  // TODO: find a better way to load this ????
  val writer = new PrintWriter("/tmp/remote.yaml", "UTF-8")
  writer.print(remoteYaml.mkString)
  writer.close()
  val titanCluster = Cluster.open("/tmp/remote.yaml")
  val gremlinClient = titanCluster.connect()
  gremlinClient.init()

  // NOTE: (CA) - This will simply fail if it already exists... furthermore we need to add more schema definition
  // we will have to add it via a new transaction.  This probably is better to keep outside in a create script of
  def createGremlinRelationshipSchema = {
    """
      |graph.tx().rollback();
      |mgmt = graph.openManagement();
      |entity = mgmt.makeVertexLabel('entity').make();
      |uri = mgmt.makePropertyKey('uri').dataType(String.class).cardinality(Cardinality.SINGLE).make();
      |wikiPageID = mgmt.makePropertyKey('wikiPageID').dataType(Long.class).cardinality(Cardinality.SINGLE).make();
      |label = mgmt.makePropertyKey('label').dataType(String.class).cardinality(Cardinality.SINGLE).make();
      |comment = mgmt.makePropertyKey('comment').dataType(String.class).cardinality(Cardinality.SINGLE).make();
      |mgmt.buildIndex('byUriComposite', Vertex.class).addKey(uri).unique().buildCompositeIndex();
      |mgmt.buildIndex('byWikiPageIDComposite', Vertex.class).addKey(wikiPageID).unique().buildCompositeIndex();
      |mgmt.buildIndex('byLabelComposite', Vertex.class).addKey(label).buildCompositeIndex();
      |mgmt.commit();
      |//Wait for the index to become available
      |mgmt.awaitGraphIndexStatus(graph, 'byUriComposite').call();
      |mgmt.awaitGraphIndexStatus(graph, 'byWikiPageIDComposite').call();
      |mgmt.awaitGraphIndexStatus(graph, 'byLabelComposite').call();
      |//Reindex the existing data
      |mgmt = graph.openManagement();
      |mgmt.updateIndex(mgmt.getGraphIndex("byUriComposite"), SchemaAction.REINDEX).get();
      |mgmt.updateIndex(mgmt.getGraphIndex("byWikiPageIDComposite"), SchemaAction.REINDEX).get();
      |mgmt.updateIndex(mgmt.getGraphIndex("byLabelComposite"), SchemaAction.REINDEX).get();
      |
      |mgmt.commit();
    """.stripMargin
  }
  gremlinClient.submitAsync(createGremlinRelationshipSchema)
  gremlinClient.closeAsync()  // keep this around as our read client..


  def valueMapStringToMap(vm:String):Map[String,String] = {
    val kv = vm.substring(1,vm.length-1).split(",")
    kv.map{ s =>
      val v = s.split("=")
      v(0).trim -> v(1).substring(1,v(1).length-1)
    }.toMap[String, String]
  }

}

