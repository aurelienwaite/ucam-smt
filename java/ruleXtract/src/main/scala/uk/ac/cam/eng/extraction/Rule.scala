package uk.ac.cam.eng.extraction

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.math.Ordering.Implicits.seqDerivedOrdering

class Rule(val source: ArrayBuffer[Symbol], val target: ArrayBuffer[Symbol]) extends Equals {

  override def toString() = {
    source.map(_.toString).mkString("_") + " " + target.map(_.toString).mkString("_")
  }
  
  def hasTwoNT() = {
    source.contains(X1)
  }
  
  def hasOneNT() = {
    source.contains(X)
  }
  
  def isPhrase() = {
    !hasOneNT && !hasTwoNT 
  }

  def canEqual(other: Any) = {
    other.isInstanceOf[uk.ac.cam.eng.extraction.Rule]
  }

  override def equals(other: Any) = {
    other match {
      case that: uk.ac.cam.eng.extraction.Rule => that.canEqual(Rule.this) && source == that.source && target == that.target
      case _                                   => false
    }
  }

  override def hashCode() = {
    val prime = 41
    prime * (prime + source.hashCode) + target.hashCode
  }

}

object S2TOrdering extends Ordering[Rule] {
  val vecOrdering = seqDerivedOrdering[ArrayBuffer,Symbol]
  
  override def compare(r1: Rule, r2: Rule) = {
    val diff = vecOrdering.compare(r1.source, r2.source)
    if (diff == 0){
      vecOrdering.compare(r1.target, r2.target)
    }else{
      diff
    }
  }
  
}