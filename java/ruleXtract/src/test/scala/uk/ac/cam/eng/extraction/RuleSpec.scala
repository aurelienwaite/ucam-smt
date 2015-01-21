package uk.ac.cam.eng.extraction

import org.scalatest._
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.DataInputStream
import java.io.ByteArrayInputStream


class RuleSpec extends FlatSpec with Matchers {

  "A rule" should "serialise and deserialise" in {
    val ruleString = "82073_X1_28500_X2_2575 8107_X1_1547_X2_205"
    val rule = new Rule(ruleString)
    val byteOut = new ByteArrayOutputStream()
    val out = new DataOutputStream(byteOut)
    rule.write(out)
    val in = new DataInputStream(new ByteArrayInputStream(byteOut.toByteArray()))
    val rule2 = new Rule()
    rule2.readFields(in)
    rule2.toString() should be (ruleString)
  } 
  
  "A rule" should "be invertiable" in {
    val ruleString = "82073_X1_28500_X2_2575 8107_X1_1547_X2_205"
    val rule = new Rule(ruleString)
    rule.isSwapping() should be (false)
    
    val ruleSwapString = "82073_X1_28500_X2_2575 8107_X2_1547_X1_205"
    val ruleSwap = new Rule(ruleSwapString)
    ruleSwap.isSwapping() should be (true)
    ruleSwap.invertNonTerminals() should not be (ruleSwap)
    val inverted = new Rule("82073_X2_28500_X1_2575 8107_X1_1547_X2_205")
    ruleSwap.invertNonTerminals() should be (inverted)
  }
}