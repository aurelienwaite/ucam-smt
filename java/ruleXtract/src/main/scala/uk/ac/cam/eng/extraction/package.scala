package uk.ac.cam.eng

package object extraction {

  // First element is the rule, second is the set of alignments found for this rule, third is the start span of the parent, and fourth is the end span of the parent
  type RuleTuple = (Rule, Alignment, Span)
  type ASet = scala.collection.mutable.HashSet[Alignment]
  type RSet = scala.collection.mutable.HashSet[Rule]
  type RulePair = (Rule, Alignment)
  
  object RuleTupleOrdering extends Ordering[RuleTuple] {
    override def compare(t1 : RuleTuple, t2 : RuleTuple) = S2TOrdering.compare(t1._1,t2._1)
  }

}