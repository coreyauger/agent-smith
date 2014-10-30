package com.nxtwv.graphs

import java.io.PrintWriter
import java.net.URL
import java.io.File
import com.nxtwv.graphs.dbpedia.{DbpediaLoader, DbPediaVariant, DbPediaThing}
import com.nxtwv.graphs.dbpedia.DbPediaThing.SchemaProperty
import com.nxtwv.utils._
import Predef._
import org.htmlcleaner.HtmlCleaner
import org.htmlcleaner.TagNode
import scala.Predef.refArrayOps
import scala.Predef.refArrayOps
import scala.collection.JavaConversions._
import play.api.libs.ws.WS
import play.api.libs.concurrent.Execution.Implicits._
import play.api.libs.functional.syntax._
import play.api.libs.json._



trait PageParser{
  val cleaner = new HtmlCleaner

  def parse(node:String)( f: (List[TagNode]) => String  )(implicit url:String):String = {
    val rootNode = cleaner.clean(new URL(url))
    f(rootNode.getElementsByName(node,true).toList)
  }
}
trait SchemaOrgParser extends PageParser{
  implicit val url = ""
  def parseSchemaOrg = parse("div")(_)
}

class Parser extends SchemaOrgParser{

}






/**
 *
 * Created by suroot on 14/10/14.
 */
object Main extends App{

  override def main(args: Array[String]) {
    // url https://schema.org/docs/full.html
    //WS.url()
    //scan("https://schema.org/docs/full.html")
    //scan("http://www.raleigh-sound.com/shows.php")

    //val jsonStr = GZipFileContents(new java.io.File("data/Colour.json.gz"), "UTF-8")
    //val jsonStr = GZipFileContents(new java.io.File("/home/suroot/projects/data/dbpedia/json/Magazine.json.gz"), "UTF-8")
    val jsonStr = GZipFileContents(new java.io.File("/home/suroot/projects/data/dbpedia/json/ComicsCharacter.json.gz"), "UTF-8")
    val json = Json.parse(jsonStr)
    val readableString: String = Json.prettyPrint(json)
    //print(readableString)
   // val writer = new PrintWriter(new File("data/Colour.json" ))
   // writer.write(readableString)
   // writer.close()

/*

*/
    val loader = new DbpediaLoader();
    loader.load(json)

    println("EXIT..")
  }




  def scan(url:String) = {
    val cleaner = new HtmlCleaner
    val props = cleaner.getProperties
    val rootNode = cleaner.clean(new URL(url))
    val elements:List[TagNode] = rootNode.getElementListHavingAttribute("itemscope", true).toList
    println(s"Found ${elements.length} elements")
    elements.foreach{
      case elem:TagNode =>
        println(elem.getText.toString)
    }
  }


  def test(url: String) = {
    val cleaner = new HtmlCleaner
    val props = cleaner.getProperties
    val rootNode = cleaner.clean(new URL(url))
    val elements:List[TagNode] = rootNode.getElementsByName("a", true).toList

    elements.foreach{
      case elem:TagNode =>
        val classType = elem.getAttributeByName("class")
        if (classType != null && classType.equalsIgnoreCase("articleTitle")) {
          println(elem.getText.toString)
          ""
        }
        println(elem.getText.toString)
    }
  }
}
