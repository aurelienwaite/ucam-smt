package uk.ac.cam.eng.extraction

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.HashMap
import scala.annotation.tailrec

case class ExtractOptions(maxSourcePhrase: Int, maxSourceElements: Int, maxTerminalLength: Int, maxNonTerminalSpan: Int, removeMonotoniceRepeats: Boolean)

object Extract {

  def toSymbols(s: String): Vector[Symbol] = {
    s.split(" ").map(x => new Terminal(x.toInt)).toVector
  }

  def extendUnalignedWords(target: Vector[Symbol], rule: Rule, ruleAlign: Alignment, senAlignment: Alignment, start: Int, end: Int): List[RuleTuple] = {
    val extendedPrev = new ListBuffer[RuleTuple]()
    var pointer = start - 1
    while (!senAlignment.isTargetAligned(pointer) && pointer >= 0) {
      extendedPrev += Tuple4(new Rule(rule.source, target.slice(pointer, end + 1).toVector),
        new ASet() += ruleAlign.incrementTrg(start - pointer), new RSet(), pointer == 0)
      pointer -= 1
    }
    val results = new ListBuffer[RuleTuple]
    pointer = end + 1
    while (!senAlignment.isTargetAligned(pointer) && pointer < target.length) {
      results += Tuple4(new Rule(rule.source, target.slice(start, pointer + 1)), new ASet() += ruleAlign, new RSet(), start == 0)
      for (prev <- extendedPrev) {
        prev match {
          case (rule, alignment, _, isStart) =>
            results += Tuple4(new Rule(rule.source, rule.target ++ target.slice(end + 1, pointer + 1)), alignment, new RSet(), isStart)
        }
      }
      pointer += 1
    }
    (extendedPrev ++ results).toList
  }

  def extractPhrasePairs(options: ExtractOptions)(source: Vector[Symbol], target: Vector[Symbol], alignment: Alignment): (List[RuleTuple], List[RuleTuple]) = {
    val results = new ListBuffer[RuleTuple]
    val extended = new ListBuffer[RuleTuple]
    val sourcePhrase = new ArrayBuffer[Symbol](options.maxSourcePhrase)
    val targetPhrase = new ArrayBuffer[Symbol](options.maxSourcePhrase)
    val phraseAlignment = new Alignment
    for (i <- 0 to source.length) {
      sourcePhrase.clear()
      phraseAlignment.clear()
      var minTargetIndex = Int.MaxValue
      var maxTargetIndex = Int.MinValue
      if (alignment.isSourceAligned(i)) {
        minTargetIndex = math.min(alignment.getS2T(i).firstKey, minTargetIndex)
        maxTargetIndex = math.max(alignment.getS2T(i).lastKey, maxTargetIndex)
      }
      //To duplicate a bug/feature we're actually using max source phrase +1
      val sourceLength = source.length - 1
      for (j <- i to sourceLength) {
        sourcePhrase += source(j)
        if (alignment.isSourceAligned(j)) {
          minTargetIndex = math.min(alignment.getS2T(j).firstKey, minTargetIndex)
          maxTargetIndex = math.max(alignment.getS2T(j).lastKey, maxTargetIndex)
        }
        if (minTargetIndex <= maxTargetIndex) {
          targetPhrase.clear()
          var consistant: Boolean = true
          for (k <- minTargetIndex to maxTargetIndex if consistant) {
            targetPhrase += target(k)
            if (alignment.isTargetAligned(k)) {
              val targetMax = alignment.getT2S(k).lastKey
              val targetMin = alignment.getT2S(k).firstKey
              if (targetMin >= i && targetMax <= j) {
                for (align <- alignment.getT2S(k)) {
                  phraseAlignment.addAlignment(align - i, k - minTargetIndex)
                }
              } else {
                consistant = false
              }
            }
          }
          if (consistant) {
            var rule = new Rule(sourcePhrase.toVector, targetPhrase.toVector)
            var alignCopy = new Alignment(phraseAlignment)
            extended ++= extendUnalignedWords(target, rule, alignCopy, alignment, minTargetIndex, maxTargetIndex)
            val tuple = Tuple4(rule, new ASet() += alignCopy, new RSet(), i == 0)
            if (alignment.isSourceAligned(i) && alignment.isSourceAligned(i + rule.source.length - 1)) {
              results += tuple
            } else {
              extended += tuple
            }
          }
        }
      }
    }
    (results.toList, extended.toList)
  }

  def filterPassNonTerminalRule(opts: ExtractOptions)(toFilter: Rule): Boolean = {
    val terminalLength = toFilter.source.segmentLength(_ match {
      case _: Terminal => true
      case _           => false
    }, 0)
    return toFilter.source.size <= opts.maxSourceElements && terminalLength <= opts.maxTerminalLength
  }

  def replace(buff: ArrayBuffer[Symbol]): ArrayBuffer[Symbol] = {
    val i1 = buff.indexOf(X)
    buff(i1) = X1
    val i2 = buff.indexOf(X)
    buff(i2) = X2
    buff
  }

  private def extract(sourceStart: Int, targetStart: Int, r: Rule, aSet: scala.collection.mutable.Set[Alignment],
                      parents: scala.collection.mutable.Set[Rule], other: Rule): List[RuleTuple] = {
    val indexSrc = r.source.indexOfSlice(other.source, sourceStart)
    val indexTrg = r.target.indexOfSlice(other.target, targetStart)
    if (indexSrc >= sourceStart && indexTrg >= targetStart) {
      val extractedSrc = new ArrayBuffer[Symbol](r.source.length)
      extractedSrc ++= r.source.take(indexSrc) += X ++= r.source.takeRight(r.source.length - indexSrc - other.source.length)
      val extractedTrg = new ArrayBuffer[Symbol](r.target.length)
      extractedTrg ++= r.target.take(indexTrg) += X ++= r.target.takeRight(r.target.length - indexTrg - other.target.length)
      if (extractedSrc.length == 1 || extractedTrg.length == 1) {
        return Nil
      }
      val adjSet = aSet.map { a =>
        val adjusted = new Alignment()
        a.s2t.foreach {
          case (src, value) => value.foreach { trg =>
            if ((src < indexSrc || src >= indexSrc + other.source.length) &&
              (trg < indexTrg || trg >= indexTrg + other.target.length)) {
              adjusted.addAlignment(if (src > indexSrc) src - other.source.length + 1 else src,
                if (trg > indexTrg) trg - other.target.length + 1 else trg)
            }
          }
        }
        adjusted
      }
      (new Rule(extractedSrc.toVector, extractedTrg.toVector), adjSet, parents+r, false) ::
        extract(indexSrc + 1, indexTrg + 1, r, aSet, parents, other)
    } else {
      Nil
    }
  }

  def extract(options: ExtractOptions, phrases: List[RuleTuple])(toExtract: List[RuleTuple]): List[RuleTuple] = {
    toExtract.flatMap {
      _ match {
        case (parent, align1, parents, _) =>
          phrases.flatMap {
            _ match {
              case (phrase2, align2, _, _) =>
                if (parent != phrase2
                  && phrase2.source.length <= options.maxNonTerminalSpan
                  && !align2.isEmpty && align2.forall(phrase2.source.length == _.s2t.size)) // The phrase must be fully aligned
                  extract(0, 0, parent, align1, parents, phrase2)
                else Nil
            }
          }
      }
    }
  }

  def extract(options: ExtractOptions)(sourceString: String, targetString: String, alignmentString: String): List[RuleTuple] = {
    val source = toSymbols(sourceString)
    val target = toSymbols(targetString)
    val alignment = new Alignment(alignmentString)
    val (phrases, extended) = extractPhrasePairs(options)(source, target, alignment)
    val extractor = extract(options, phrases)_
    val forExtraction = phrases ++ extended.filter(_ match {
      case (p, a, _, isStart) => a.head.isSourceAligned(p.source.length - 1) && (!isStart || (isStart && a.head.isSourceAligned(0))) &&
        a.head.isTargetAligned(p.target.length - 1) && a.head.isTargetAligned(0)
    })
    val rules1NT = extractor(forExtraction)
    val buff = new ArrayBuffer[Symbol]()
    val rules2NT = extractor(rules1NT).map(_ match {
      case (r, a, parents, _) => {
        buff.clear()
        buff ++= r.source
        val source = replace(buff).toVector
        buff.clear()
        buff ++= r.target
        val target = replace(buff).toVector
        (new Rule(source, target), a, parents, false)
      }
    }).filterNot(_ match { // Filtering monotonic repeats
      case (r, _, _, _) => options.removeMonotoniceRepeats &&
        (r.source.containsSlice(Seq(X1, X2)) || r.target.containsSlice(Seq(X1, X2)))
    })
    (phrases ++ extended).filter(_ match {
      // We have to filter phrase pairs here so that source length is not greater than max source length, but not hiero rules!
      case (phrase, _, _, _) => phrase.source.length <= options.maxSourcePhrase
    }) ++ ((rules1NT ++ rules2NT).filter(_ match {
      case (rule, _, _, _) => filterPassNonTerminalRule(options)(rule)
    }).sorted(RuleTupleOrdering).foldRight(List[RuleTuple]())((t, tList) => tList match {
      case Nil => t :: Nil
      case head :: tail => {
        head match {
          case (hRule, hAlignments, hPhrases, _) =>
            t match {
              case (rule, alignments, phrases, _) => {
                if (hRule == rule) {
                  hAlignments ++= alignments
                  hPhrases ++= phrases
                  head :: tail
                } else {
                  t :: (head :: tail)
                }
              }
            }
        }

      }
    }))
  }

  def extractWithDupes(options: ExtractOptions)(sourceString: String, targetString: String, alignmentString: String): List[(Rule, scala.collection.mutable.Set[Alignment])] = {
    val results = extract(options)(sourceString, targetString, alignmentString)
    results.map {
      _ match {
        case (r, aSet, parents, _) => {
          if (r.hasTwoNT && (r.source.head != X1 || r.source.last != X2))
            Tuple3(r, aSet,
              parents.filter(_.hasOneNT).foldRight(0)((infix, repeats) =>
                if (parents.exists(parent => infix != parent
                  && parent.source.containsSlice(infix.source))) repeats + 1 else repeats))
          else if (r.hasOneNT){
             val repeats = parents.filter(_.isPhrase).foldRight(new RSet += parents.head)((r1, rSet)=>{
               val out = new RSet() ++=rSet
               var found = false
               for(r2 <- rSet){
                 if(r1.source.containsSlice(r2.source)){
                   out -= r2
                   out += r1
                   found = true
                 }
                 else if (r2.source.containsSlice(r1.source)){
                   found = true
                 }
               }
               if(!found)
                 out += r1
               else
                 out
             })
             (r,aSet, repeats.size -1)
          }
          else
            (r, aSet, 0)
        }
      }
    } flatMap {
      _ match {
        case (r, aSet, infixCount) => Seq.fill(infixCount + 1)((r, aSet))
      }
    }
  }
}