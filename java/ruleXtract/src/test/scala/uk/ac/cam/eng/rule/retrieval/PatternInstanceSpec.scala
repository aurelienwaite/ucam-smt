package uk.ac.cam.eng.rule.retrieval

import org.scalatest._
import scala.collection.JavaConversions.asJavaCollection
import scala.collection.JavaConverters._
import uk.ac.cam.eng.util.CLI.RuleRetrieverParameters
import java.util.ArrayList
import scala.collection.immutable.Set
import uk.ac.cam.eng.extraction.Rule

class PatternInstanceSpec extends FlatSpec with Matchers {

  "Pattern instances" should "be extracted from a sentence" in {
    val sourcePatterns: java.util.Collection[SidePattern] =  List(SidePattern.parsePattern("W_X"),
      SidePattern.parsePattern("X_W"),
      SidePattern.parsePattern("W_X_W"),
      SidePattern.parsePattern("X1_W_X2"),
      SidePattern.parsePattern("X2_W_X1"),
      SidePattern.parsePattern("W_X1_W_X2"),
      SidePattern.parsePattern("W_X2_W_X1"),
      SidePattern.parsePattern("X1_W_X2_W"),
      SidePattern.parsePattern("X2_W_X1_W"),
      SidePattern.parsePattern("W_X1_W_X2_W"),
      SidePattern.parsePattern("W_X2_W_X1_W"))
    val params = new RuleRetrieverParameters
    params.hr_max_height = 10
    params.rp.maxNonTerminalSpan = 10
    params.rp.maxSourceElements = 5
    params.rp.maxTerminalLength = 5
    params.rp.maxSourcePhrase = 9
    val creator = new PatternInstanceCreator(params, sourcePatterns)
    
    val sentence = "16055 3 102 5182 66 18 23602 12611 5 6522 2377 3431 3 98 52858 61 46 2140 4422 15871 25 67408 17658 26 1731 19663 4"
    val result  =  asScalaSetConverter(creator.createSourcePatternInstances(sentence)).asScala
    println(result.map{ r  => r.toString + " " + r.source.toPattern()}.mkString("\n"))
  }

}