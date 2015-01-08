package uk.ac.cam.eng.extraction

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.HashMap
import scala.annotation.tailrec
import scala.collection.immutable.HashSet
import scala.collection.immutable.Stream

case class ExtractOptions(maxSourcePhrase: Int, maxSourceElements: Int, maxTerminalLength: Int, maxNonTerminalSpan: Int, removeMonotoniceRepeats: Boolean)

object Extract {

  def toSymbols(s: String): Vector[Symbol] = {
    s.split(" ").map(x => new Terminal(x.toInt)).toVector
  }

  def splitPhrasePairs(results: Seq[RuleTuple]): (Seq[RulePair], Seq[RulePair]) = {
    val parentPhrases = new ListBuffer[RulePair]
    val otherPhrases = new ListBuffer[RulePair]
    for (tuple <- results) {
      tuple match {
        case (r, a, start, _, _, _) => {
          val pair = (r, new ASet += a)
          val isStart = start == 0
          if (a.isSourceAligned(r.source.length - 1) && (!isStart || (isStart && a.isSourceAligned(0))) &&
            a.isTargetAligned(r.target.length - 1) && a.isTargetAligned(0)) {
            parentPhrases += pair
          } else {
            otherPhrases += pair
          }
        }
      }
    }
    return (parentPhrases, otherPhrases)
  }

  def consistencyCheck(alignment: Alignment, sourceStart: Int, sourceEnd: Int, targetEnd: Int): Boolean = {
    val sourceMax = alignment.getT2S(targetEnd).last
    val sourceMin = alignment.getT2S(targetEnd).head
    sourceMin >= sourceStart && sourceMax <= sourceEnd
  }

  def extractPhrasePairs(options: ExtractOptions)(source: Seq[Symbol], target: Seq[Symbol], alignment: Alignment,
                                                  firstPass: Boolean): Seq[RuleTuple] = {
    val sourcePhrase = new ArrayBuffer[Symbol](options.maxSourcePhrase)
    val targetPhrase = new ArrayBuffer[Symbol](options.maxSourcePhrase)
    val results = new ListBuffer[RuleTuple]
    val phraseAlignment = new Alignment
    for (i <- 0 to source.length) {
      sourcePhrase.clear()
      var minTargetIndex = Int.MaxValue
      var maxTargetIndex = Int.MinValue
      if (alignment.isSourceAligned(i)) {
        minTargetIndex = math.min(alignment.getS2T(i).head, minTargetIndex)
        maxTargetIndex = math.max(alignment.getS2T(i).last, maxTargetIndex)
      }
      var sourceFullyAligned = true
      //For heiro we consider phrases with max length of the sentences. The long phrases are filtered out at the end.
      val sourceLength = if (firstPass) source.length - 1 else Math.min(i + options.maxNonTerminalSpan, source.length - 1)
      for (j <- i to sourceLength) {
        if (sourceFullyAligned || firstPass){
          sourcePhrase += source(j)
          if (alignment.isSourceAligned(j)) {
            minTargetIndex = math.min(alignment.getS2T(j).head, minTargetIndex)
            maxTargetIndex = math.max(alignment.getS2T(j).last, maxTargetIndex)
          } else
            sourceFullyAligned = false
          if (minTargetIndex <= maxTargetIndex) {
            targetPhrase.clear()
            var targetFullyAligned = true
            var consistant = true
            for (k <- minTargetIndex to maxTargetIndex) {
              if (consistant) {
                targetPhrase += target(k)
                if (alignment.isTargetAligned(k))
                  consistant = consistencyCheck(alignment, i, j, k)
                else
                  targetFullyAligned = false
              }
            }
            if (consistant) {
              var rule = new Rule(new ArrayBuffer ++= sourcePhrase, new ArrayBuffer ++= targetPhrase)
              var phraseAlignment = new Alignment
              if (firstPass) {
                phraseAlignment = alignment.extractPhraseAlignment(i, j, minTargetIndex)
              }
              val tuple = Tuple6(rule, phraseAlignment, i, j, minTargetIndex, maxTargetIndex)
              if (firstPass) {
                results ++= extendUnalignedWords(target, rule, phraseAlignment, alignment, minTargetIndex,
                  maxTargetIndex, i, j)
                results += tuple
              } else if (sourceFullyAligned)
                results += tuple
            }
          }
        }
      }
    }
    return results
  }

  /**
   * Note the target spans of the tuples from this function are not set!
   */
  private def extendUnalignedWords(target: Seq[Symbol], rule: Rule, ruleAlign: Alignment, senAlignment: Alignment,
                                   start: Int, end: Int, sourceSpanStart: Int, sourceSpanEnd: Int): List[RuleTuple] = {
    val extendedPrev = new ListBuffer[RuleTuple]()
    var pointer = start - 1
    while (!senAlignment.isTargetAligned(pointer) && pointer >= 0) {
      extendedPrev += Tuple6(new Rule(rule.source, new ArrayBuffer ++= target.slice(pointer, end + 1)),
        ruleAlign.incrementTrg(start - pointer), sourceSpanStart, sourceSpanEnd, -1, -1)
      pointer -= 1
    }
    val results = new ListBuffer[RuleTuple]
    pointer = end + 1
    while (!senAlignment.isTargetAligned(pointer) && pointer < target.length) {
      results += Tuple6(new Rule(rule.source, new ArrayBuffer ++= target.slice(start, pointer + 1)),
        ruleAlign, sourceSpanStart, sourceSpanEnd, -1, -1)
      for (prev <- extendedPrev) {
        prev match {
          case (rule, alignment, start, end, _, _) =>
            results += Tuple6(new Rule(rule.source, rule.target ++ target.slice(end + 1, pointer + 1)),
              alignment, start, end, -1, -1) // Extended phrases are never used as constituent child phrases, therefore we don't need target spans.
        }
      }
      pointer += 1
    }
    (extendedPrev ++ results).toList
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

  def replace(x: Symbol, y: Symbol, pair: RulePair): RulePair = {
    pair match {
      case (r, a) =>
        (new Rule(r.source.map { sym => if (sym == x) y else sym }, r.target), a)
    }
  }

  def extractNT(r: Rule, a: Alignment,
                filterFunc: Rule => Boolean, options: ExtractOptions): Seq[RulePair] = {
    val phrasePairs = extractPhrasePairs(options)(r.source, r.target, a, false)
    phrasePairs.flatMap {
      case (phrase, _, srcStartSpan, srcEndSpan, trgStartSpan, trgEndSpan) => {
        //Target phrase may contain X because it's allowed to contain unaligned tokens
        if (phrase.source.length <= options.maxNonTerminalSpan && !phrase.target.contains(X)) {
          val extractedSrc = new ArrayBuffer[Symbol](r.source.length)
          extractedSrc ++= r.source.take(srcStartSpan) += X ++= r.source.takeRight(r.source.length - srcEndSpan - 1)
          val extractedTrg = new ArrayBuffer[Symbol](r.target.length)
          extractedTrg ++= r.target.take(trgStartSpan) += X ++= r.target.takeRight(r.target.length - trgEndSpan - 1)
          val result = new Rule(extractedSrc, extractedTrg)
          if (extractedSrc.length != 1 && extractedTrg.length != 1 && filterFunc(result)) {
            val adjusted = a.adjustExtractedAlignments(srcStartSpan, srcEndSpan, trgStartSpan, trgEndSpan, phrase)
            List((result, new ASet + adjusted))
          } else
            Nil
        } else
          Nil
      }
    }
  }

  def extractNT(options: ExtractOptions)(toExtract: List[RulePair],
                                         filterFunc: Rule => Boolean): Stream[Seq[RulePair]] = {
    toExtract match {
      case Nil => Stream.Empty
      case head :: tail => {
        var rules = head match {
          case (r, a) => extractNT(r, a.head, filterFunc, options)
        }
        Stream.cons(rules, extractNT(options)(tail, filterFunc))
      }
    }
  }

  def removeDupes1NT(results: Stream[Seq[RulePair]]): Seq[RulePair] = {
    val removed = (new scala.collection.mutable.HashSet[RulePair])
    for (pairs <- results) {
      removed ++= pairs
    }
    removed.toList
  }

  def removeDupes2NT(results: Stream[Seq[RulePair]]): List[RulePair] = {
    val removed = new scala.collection.mutable.HashSet[RulePair]
    val notRemovedWithCounts = new scala.collection.mutable.HashMap[RulePair, Int]
    val ntSeq = List(X)
    for (result <- results) {
      for (pair <- result) {
        pair match {
          case (r, a) => {
            if (r.source.startsWith(ntSeq) && r.source.endsWith(ntSeq))
              removed += pair
            else {
              val count = notRemovedWithCounts.getOrElse(pair, 0) + 1
              notRemovedWithCounts.put(pair, count)
            }
          }
        }
      }
    }
    val notRemoved = new ListBuffer[RulePair]
    for (entry <- notRemovedWithCounts) {
      entry match {
        case (pair, count) =>
          notRemoved ++= Seq.fill(count - 1)(pair)
      }
    }
    (notRemoved ++= removed).toList
  }

  def filter2NT(options: ExtractOptions)(r: Rule): Boolean = {
    val removeMonotonic = options.removeMonotoniceRepeats &&
      (r.source.containsSlice(Seq(X, X)) || r.target.containsSlice(Seq(X, X)))
    return !removeMonotonic && filterPassNonTerminalRule(options)(r)
  }

  def extract(options: ExtractOptions)(sourceString: String, targetString: String, alignmentString: String): Seq[RulePair] = {
    val source = toSymbols(sourceString)
    val target = toSymbols(targetString)
    val alignment = new Alignment(alignmentString)
    val (parents, other) = splitPhrasePairs(extractPhrasePairs(options)(source, target, alignment, true))
    //println(children.map { _._1.toString }.toList.sorted.mkString("\n"))
    //println(parents.map { _.toString }.toList.sorted.mkString("\n"))
    val extractor = extractNT(options)_
    val rules1NT = removeDupes1NT(extractor(parents.toList, _ => true))
    //println(rules1NT.map { _.toString }.toList.sorted.mkString("\n"))
    val buff = new ArrayBuffer[Symbol]()
    val extracted2NT = extractor(rules1NT.toList, filter2NT(options)_)
    val rules2NT = removeDupes2NT(extracted2NT).map { pair =>
      {
        pair match {
          case (r, a) => {
            if (r.source.contains(X))
              (new Rule(replace(r.source), replace(r.target)), a)
            else
              pair
          }
        }
      }
    }
    (parents ++ other)
      .filter { case (r, _) => r.source.size <= options.maxSourcePhrase } ++
      rules1NT.filter { case (r, _) => filterPassNonTerminalRule(options)(r) } ++ rules2NT
  }

}