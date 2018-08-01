package streaming

import scala.collection.convert.wrapAll._
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

object NLPUtils {

  def getSentimentRating(text:String):Double = {


    //pipeline properties
    val properties = new Properties()
    properties.setProperty("annotators", "tokenize, ssplit, parse, sentiment")

    //build pipeline
    val pipeline:StanfordCoreNLP = new StanfordCoreNLP(properties)

    //create document object
    val document:CoreDocument = new CoreDocument(text)

    //annotate the document
    pipeline.annotate(document)

    val coreMaps = document.annotation()
      .get(classOf[CoreAnnotations.SentencesAnnotation])

    val sentiments = coreMaps
      .map(cm => cm.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree]))
      .map(tree => {
        val sentimentRating = RNNCoreAnnotations.getPredictedClass(tree)

        //return sentimentRating
        sentimentRating

        //alternatively return value {-1, 0, 1} for {negative, neutral, positive}
//        sentimentRating match {
//          case x if x == 0 || x == 1 => -1
//          case 2 => 0
//          case x if x == 3 || x == 4 => 1
//        }
      })

    //calculate overall sentiment
    var sum = 0
    for (sentiment <- sentiments) {
      sum = sum + sentiment
    }

    val avg:Double = sum.toDouble / sentiments.size

    avg
  }


}
