import java.io._
import scala.xml.XML
import scalaj.http._
import play.api.libs.json._

// COMP9313 19T2 Assignment 3
// Student z5146092

object CaseIndex {

    // File reader function - read all xml file in the directory
    def readFile(file : File): Array[File] = {
         file.listFiles().filter(! _.isDirectory)
             .filter(t => t.toString.endsWith(".xml"))
    }

    def main(args: Array[String]): Unit = {

        // Create elastic search index - provide search route
        val elasticSearchIndex = Http("http://localhost:9200/legal_idx")
            .method("PUT")
            .header("Content-Type", "application/json")
            .option(HttpOptions.readTimeout(10000)).asString

        // Create elastic search post information - entity details of such xml files
        // Labels are separated and stored in elastic search, indexes are created afterwards
        val mapping_response = Http("http://localhost:9200/legal_idx/cases/_mapping?pretty")
            .postData(
                """{"cases":{"properties":{"filename":{"type":"text"},
                  |"title":{"type":"text"},"austlii":{"type":"text"},
                  |"catchphrase":{"type":"text"},"sentence":{"type":"text"},
                  |"person":{"type":"text"},"location":{"type":"text"},
                  |"organization":{"type":"text"}}}}""".stripMargin('|'))
            .method("PUT")
            .header("Content-Type", "application/json")
            .option(HttpOptions.readTimeout(10000)).asString

        // Read file and extract information from files
        readFile(new File(args(0))).foreach(xmlFile => {

            val fileContent = XML.loadFile(xmlFile)     // XML parsed file
            val fileName: String = xmlFile.getName      // The file name
            val title = (fileContent \ "name").text.mkString        // The title of the file (label "name")
            val austlii = (fileContent \ "AustLII").text.mkString   // The URL of the file (label "AustLII")
            val catchPhrase = (fileContent \\ "catchphrases" ).text // The catch phrase (label "catchphrases")
                .split("\n")
                .filter(element => !element.isEmpty)
                .map(element => "\'" + element + "\'")
                .toList

            val sentences = (fileContent \\ "sentences").text   // The sentences of the file (label "sentences")
                .filter(line => line >= ' ')
            var sentence_payload: List[String] = List()         // The sentences payload, cured with single quote

            (fileContent \\ "sentences" \ "sentence").foreach(sentence => {
                sentence_payload = sentence_payload :+ "\'" + sentence.text.replace("\"","\\\"")
                    .filter(_>=' ') + "\'"
            })

            // CoreNLP request - parsing useful information (person, organization, location) from sentences
            val nlpUrl = """http://localhost:9000/?properties=%7B""" +
                """'annotators':'ner','ner.applyFineGrained':'false','outputFormat':'json'%7D"""

            val nlpResponse = Http(nlpUrl.stripMargin)
                .postData(sentences)
                .method("POST")
                .header("Content-Type", "application/json")
                .option(HttpOptions.readTimeout(60000)).asString.body

            // The indexes
            var person = List[String]()
            var organization = List[String]()
            var location = List[String]()

            // Get information from CoreNLP
            // Extract text based on their label (i.e. label "ner")
            // Then gather indexes from originalText to the index lists above
            (Json.parse(nlpResponse) \\ "sentences").foreach(entity => {
                val nerLabel = entity \\ "ner"
                val texts = entity \\ "originalText"

                // Check each label, if matched, store it to corresponding index list
                for (i <- 0 to nerLabel.length - 1) {
                    if (nerLabel(i).toString.contains("PERSON") &&
                        !person.contains(texts(i).toString)) {
                        person = person :+ texts(i).toString.replace("\"", "\'")
                    }
                    else if (nerLabel(i).toString.contains("ORGANIZATION") &&
                        !organization.contains(texts(i).toString)) {
                        organization = organization :+ texts(i).toString.replace("\"", "\'")
                    }
                    else if (nerLabel(i).toString.contains("LOCATION") &&
                        !location.contains(texts(i).toString)) {
                        location = location :+ texts(i).toString.replace("\"", "\'")
                    }
                }
            })

            // The payload generated for elastic search index creation
            val postPayload =
                s"""{"filename":"${fileName}","title":"${title}","austlii":"${austlii}",
                   |"catchphrase":"${"[" + catchPhrase.mkString(",") + "]"}",
                   |"sentence":"${"[" + sentence_payload.mkString(",") + "]"}",
                   |"person":"${"[" + person.mkString(",") + "]"}",
                   |"location":"${"[" + location.mkString(",") + "]"}",
                   |"organization":"${"[" + organization.mkString(",") + "]"}"}"""
                    .stripMargin

            // Use post method to create index and entity on elastic search
            val index_result = Http("http://localhost:9200/legal_idx/cases/"+fileName+"?pretty")
                .postData(postPayload).method("POST")
                .header("Content-Type", "application/json")
                .option(HttpOptions.readTimeout(10000)).asString

            print(index_result)     // If created, then the program works.
        })
    }
}
