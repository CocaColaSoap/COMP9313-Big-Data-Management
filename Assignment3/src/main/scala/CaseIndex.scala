import java.io.File

import play.api.libs.json._
import scalaj.http._

import scala.xml.XML


object CaseIndex extends Serializable(){
  //read xml files from given file path
  def readFiles(files: File): List[File] ={
    //check if file is readable
    if(files.exists && files.isDirectory) {
      //read xml files
      files.listFiles.filter(file=> file.toString.endsWith(".xml")).toList
    }
    else{
      List[File]()
    }
  }

  def main(args:Array[String]) {
    print(args)
    val inputFilePath = args(0)
    val files = new File(inputFilePath)
    //create index
    val indexmessage = Http("http://localhost:9200/legal_idx").method("PUT").header("Content-Type", "application/json").option(HttpOptions.readTimeout(10000)).asString
    println(indexmessage)
    //create index mapping
    val mappingmessage = Http("http://localhost:9200/legal_idx/cases/_mapping?pretty")
      .postData(
        """{"cases":{"properties":{
          |"file name":{"type":"text"},
          |"case title":{"type":"text"},
          |"original source url":{"type":"text"},
          |"catchphrase":{"type":"text"},
          |"sentences":{"type":"text"},
          |"person":{"type":"text"},
          |"location":{"type":"text"},
          |"organization":{"type":"text"}}}}""".stripMargin).method("PUT").header("Content-Type", "application/json").option(HttpOptions.readTimeout(10000)).asString
    println(mappingmessage)
    readFiles(files).foreach(file=>{
      //read xml file
      val xml_content = XML.loadFile(file)
      //Get file name
      val file_name = file.getName
      //Get content in label <name> from xml content as case title
      val title = (xml_content \ "name").text
      //Get content in label <AustLII> from xml content as case source
      val url = (xml_content \ "AustLII").text
      //Get content in label <catchphrases> from xml content and process to a list
      val catchphrase= "["+(xml_content \\ "catchphrases").text.split("\n").filter(line=> !line.isEmpty).map(line => "\'"+line+"\'").mkString(",")+"]"
      //Get content in label <sentences>
      val file_sentences = (xml_content \\ "sentences" \ "sentence")
      //Transfer label<sentences> contents to List
      var sentences : List[String] = List()
      file_sentences.foreach(sentence => {
        sentences = sentences :+"\'"+sentence.text.replace("\"","\\\"").filter(_>=' ')+"\'"

      })
      //new List to receive analyzed data from NLP result.
      var person : List[String] = List()
      var location: List[String] = List()
      var organization: List[String] = List()
      //make file_sentence to string and send as data need to be analyzed to corenlp server
      val NLP_result = Http("""http://localhost:9000/?properties=%7B'annotators':'ner','ner.applyFineGrained':'false','outputFormat':'json'%7D""").postData(file_sentences.text.replace("\"","\\\"")
        .split("\n").filter(line=>(!line.isEmpty)).mkString).method("POST").header("Content-Type", "application/json").option(HttpOptions.readTimeout(60000)).asString.body

      //transfer NLP_result(type string) to json format for later analysis
      (Json.parse(NLP_result) \\ "sentences").foreach(line =>{
        val feature = line \\ "ner"
        val text = line \\ "originalText"
        //assort analysed data and adding them to list
        for(i <- 0 until feature.length){
          if(feature(i).toString().contains("PERSON")){
            person = (person :+ text(i).toString().replace("\"","\'")).distinct
          }
          if(feature(i).toString().contains("LOCATION")){
            location = (location :+ text(i).toString().replace("\"","\'")).distinct
          }
          if(feature(i).toString().contains("ORGANIZATION")){
            organization = (organization :+ text(i).toString().replace("\"","\'")).distinct
          }
        }
      })
      //combine all the elements as Data for sending
      val Data = s"""{"file_name":"${file_name}","case_title":"${title}","original_source_url":"${url}","catchphrase":"${catchphrase}","sentences":"${"["+sentences.mkString(",")+"]"}","person":"${"["+person.mkString(",")+"]"}","location":"${"["+location.mkString(",")+"]"}","organization":"${"["+organization.mkString(",")+"]"}"}"""
      //update index
      val updatemessage = Http("http://localhost:9200/legal_idx/cases/?pretty").postData(Data).method("POST").header("Content-Type", "application/json").option(HttpOptions.readTimeout(10000)).asString
      println(updatemessage)
    })
  }
}