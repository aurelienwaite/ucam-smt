package uk.ac.cam.eng

package object extraction {

  // First element is the rule, second is the set of alignments found for this rule, and third is the set of phrases used to generate it
  type RuleTuple = (Rule, scala.collection.mutable.Set[Alignment], scala.collection.mutable.Set[Rule], Boolean)
  type ASet = scala.collection.mutable.HashSet[Alignment]
  type RSet = scala.collection.mutable.HashSet[Rule]

  object RuleTupleOrdering extends Ordering[RuleTuple] {
    override def compare(t1 : RuleTuple, t2 : RuleTuple) = S2TOrdering.compare(t1._1,t2._1)
  }

}