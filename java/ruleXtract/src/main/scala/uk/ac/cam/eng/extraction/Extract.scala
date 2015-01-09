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

  def splitPhrasePairs(results: Seq[(Span, Span)], a: Alignment, options: ExtractOptions): (Seq[(Span, Span)], Seq[(Span, Span)], Seq[(Span, Span)]) = {
    val parentPhrases = new ListBuffer[(Span, Span)]
    val childPhrases = new ListBuffer[(Span, Span)]
    val otherPhrases = new ListBuffer[(Span, Span)]
    for (pair <- results) {
      pair match {
        case (srcSpan, trgSpan) => {
          val isStart = srcSpan.start == 0
          if (a.isSourceAligned(srcSpan.end) && (!isStart || (isStart && a.isSourceAligned(0))) &&
            a.isTargetAligned(trgSpan.end) && a.isTargetAligned(trgSpan.start)) {
            parentPhrases += pair
            var srcAligned = true
            for (i <- srcSpan.start to srcSpan.end if srcAligned) {
              srcAligned &= a.isSourceAligned(i)
            }
            if (srcAligned && srcSpan.end - srcSpan.start +1 <= options.maxNonTerminalSpan) {
              childPhrases += pair
            }
          } else {
            otherPhrases += pair
          }
        }
      }
    }
    return (parentPhrases, childPhrases, otherPhrases)
  }

  def consistencyCheck(alignment: Alignment, sourceStart: Int, sourceEnd: Int, targetEnd: Int): Boolean = {
    val sourceMax = alignment.getT2S(targetEnd).last
    val sourceMin = alignment.getT2S(targetEnd).head
    sourceMin >= sourceStart && sourceMax <= sourceEnd
  }

  def extractPhrasePairs(options: ExtractOptions)(source: Seq[Symbol], target: Seq[Symbol], alignment: Alignment): (Seq[(Span, Span)], Seq[(Span, Span)], Seq[(Span, Span)]) = {
    val results = new ListBuffer[(Span, Span)]
    for (i <- 0 to source.length) {
      var minTargetIndex = Int.MaxValue
      var maxTargetIndex = Int.MinValue
      if (alignment.isSourceAligned(i)) {
        minTargetIndex = math.min(alignment.getS2T(i).head, minTargetIndex)
        maxTargetIndex = math.max(alignment.getS2T(i).last, maxTargetIndex)
      }
      //For heiro we consider phrases with max length of the sentences. The long phrases are filtered out at the end.
      val sourceLength = source.length - 1
      for (j <- i to sourceLength) {
        if (alignment.isSourceAligned(j)) {
          minTargetIndex = math.min(alignment.getS2T(j).head, minTargetIndex)
          maxTargetIndex = math.max(alignment.getS2T(j).last, maxTargetIndex)
        }
        if (minTargetIndex <= maxTargetIndex) {
          var targetFullyAligned = true
          var consistant = true
          for (k <- minTargetIndex to maxTargetIndex) {
            if (consistant) {
              if (alignment.isTargetAligned(k))
                consistant = consistencyCheck(alignment, i, j, k)
              else
                targetFullyAligned = false
            }
          }
          if (consistant) {
            val srcSpan = new PhraseSpan(i, j)
            val trgSpan = new PhraseSpan(minTargetIndex, maxTargetIndex)
            val tuple = Tuple2(srcSpan, trgSpan)
            results ++= extendUnalignedWords(alignment, trgSpan, srcSpan, target.length)
            results += tuple
          }
        }

      }
    }
    return splitPhrasePairs(results, alignment, options)
  }

  /**
   * Note the target spans of the tuples from this function are not set!
   */
  private def extendUnalignedWords(senAlignment: Alignment,
                                   trgSpan: Span, span: Span, trgLength: Int): List[(Span, Span)] = {
    val extendedPrev = new ListBuffer[(Span, Span)]()
    var pointer = trgSpan.start - 1
    while (!senAlignment.isTargetAligned(pointer) && pointer >= 0) {
      extendedPrev += Tuple2(span, new PhraseSpan(pointer, trgSpan.end))
      pointer -= 1
    }
    val results = new ListBuffer[(Span, Span)]
    pointer = trgSpan.end + 1
    while (!senAlignment.isTargetAligned(pointer) && pointer < trgLength) {
      results += Tuple2(span, new PhraseSpan(trgSpan.start, pointer))
      for (prev <- extendedPrev) {
        prev match {
          case (s, t) =>
            results += Tuple2(s, new PhraseSpan(t.start, pointer))
        }
      }
      pointer += 1
    }
    (extendedPrev ++ results).toList
  }

  def filterPassNonTerminalRule(opts: ExtractOptions)(toFilter: (Span, Span)): Boolean = {
    val (terminalLength, sourceElements) = toFilter match {
      case (srcSpan: OneNTSpan, _) => {
        val terminalLength = srcSpan.startX - srcSpan.start + srcSpan.end - srcSpan.endX
        (terminalLength, terminalLength + 1)
      }
      case (srcSpan: TwoNTSpan, _) => {
        val terminalLength = srcSpan.startX - srcSpan.start + srcSpan.startX2-1 - srcSpan.endX +
          srcSpan.end - srcSpan.endX2
        (terminalLength, terminalLength + 2)
      }
    }
    return sourceElements <= opts.maxSourceElements && terminalLength <= opts.maxTerminalLength
  }

  def constituent(parent: (Span, Span), child: (Span, Span), options: ExtractOptions): Boolean = {
    child match {
      case (cSrcSpan, cTrgSpan) => {
        parent match {
          case (pSrcSpan: PhraseSpan, pTrgSpan: PhraseSpan) => {
            val notSameSpan = (pSrcSpan.start != cSrcSpan.start || pSrcSpan.end != cSrcSpan.end) &&
              (pTrgSpan.start != cTrgSpan.start || pTrgSpan.end != cTrgSpan.end)
            val inRange = cSrcSpan.start >= pSrcSpan.start && cSrcSpan.end <= pSrcSpan.end
            notSameSpan && inRange

          }
          case (pSrcSpan: OneNTSpan, pTrgSpan: OneNTSpan) => {
            val inRange = (cSrcSpan.start >= pSrcSpan.start && cSrcSpan.end < pSrcSpan.startX) ||
              (cSrcSpan.start > pSrcSpan.endX && cSrcSpan.end <= pSrcSpan.end)
            val isMonotonicRepeat = options.removeMonotoniceRepeats &&
              (cSrcSpan.end + 1 == pSrcSpan.startX || cSrcSpan.start - 1 == pSrcSpan.endX ||
                  cTrgSpan.end + 1 == pTrgSpan.startX || cTrgSpan.start - 1 == pTrgSpan.endX)
            inRange && !isMonotonicRepeat
          }
          case _ => false
        }
      }
    }
  }

  def deduperKey(srcSpan: Span): Span = {
    srcSpan match {
      case srcSpan: OneNTSpan => {
        if (srcSpan.start == srcSpan.startX)
          new OneNTSpan(srcSpan.endX, srcSpan.end, srcSpan.endX, srcSpan.endX)
        else if (srcSpan.end == srcSpan.endX)
          new OneNTSpan(srcSpan.start, srcSpan.startX, srcSpan.startX, srcSpan.startX)
        else
          srcSpan
      }
      case srcSpan: TwoNTSpan => {
        if (srcSpan.start == srcSpan.startX && srcSpan.end == srcSpan.endX2) {
          TwoNTSpan(srcSpan.endX, srcSpan.startX2, srcSpan.endX, 
              srcSpan.endX,srcSpan.startX2, srcSpan.startX2)
        }else
          srcSpan
      }
      case _ => new PhraseSpan(-1, -1)
    }

  }

  def extractNT(children: Seq[(Span, Span)], toExtract: (Span, Span),
                filterFunc: ((Span, Span)) => Boolean, options: ExtractOptions,
                deduper: scala.collection.mutable.HashSet[Span]): Seq[(Span, Span)] = {
    val constituentPhrases = children.filter(constituent(toExtract, _, options))
    toExtract match {
      case (pSrcSpan, pTrgSpan) => {
        constituentPhrases.flatMap {
          case (srcSpan, trgSpan) => {
            val constituentSrc = pSrcSpan.createSpan(srcSpan)
            val rule = (constituentSrc, pTrgSpan.createSpan(trgSpan))
            val key = deduperKey(constituentSrc)
            if (!deduper.contains(key) && filterFunc(rule)) {
              deduper += key
              List(rule)
            } else
              Nil
          }
        }
      }
    }
  }

  def extractNT(options: ExtractOptions, children: Seq[(Span, Span)], deduper: scala.collection.mutable.HashSet[Span])(toExtract: List[(Span, Span)], filterFunc: ((Span, Span)) => Boolean): Stream[Seq[(Span, Span)]] = {
    toExtract match {
      case Nil => Stream.Empty
      case head :: tail => {
        val NTs = extractNT(children, head, filterFunc, options, deduper)
        Stream.cons(NTs, extractNT(options, children, deduper)(tail, filterFunc))
      }
    }
  }

  def transformSpan(source: Vector[Symbol], target: Vector[Symbol], alignment: Alignment, span: (Span, Span)): RulePair = {
    var phraseAlignment = alignment.extractPhraseAlignment(span)
    span match {
      case (src: PhraseSpan, trg: PhraseSpan) => {
        val r = new Rule(new ArrayBuffer ++= source.slice(src.start, src.end + 1),
          new ArrayBuffer ++= target.slice(trg.start, trg.end + 1))
        (r, phraseAlignment)
      }
      case (src: OneNTSpan, trg: OneNTSpan) => {
        val srcString = new ArrayBuffer ++= source.slice(src.start, src.startX) +=
          X ++= source.slice(src.endX + 1, src.end + 1)
        val trgString = new ArrayBuffer ++= target.slice(trg.start, trg.startX) +=
          X ++= target.slice(trg.endX + 1, trg.end + 1)
        (new Rule(srcString, trgString), phraseAlignment)
      }
      case (src: TwoNTSpan, trg: TwoNTSpan) => {
        val srcString = new ArrayBuffer ++= source.slice(src.start, src.startX) +=
          X1 ++= source.slice(src.endX + 1, src.startX2) += X2 ++=
          source.slice(src.endX2 + 1, src.end + 1)
        val trgString = if (trg.startX < trg.startX2)
          new ArrayBuffer ++= target.slice(trg.start, trg.startX) +=
            X1 ++= target.slice(trg.endX + 1, trg.startX2) += X2 ++=
            target.slice(trg.endX2 + 1, trg.end + 1)
        else
          new ArrayBuffer ++= target.slice(trg.start, trg.startX2) +=
            X2 ++= target.slice(trg.endX2 + 1, trg.startX) += X1 ++=
            target.slice(trg.endX + 1, trg.end + 1)
        (new Rule(srcString, trgString), phraseAlignment)
      }
      case _ => throw new UnsupportedOperationException("Must use spans of the same type")
    }
  }

  def spansToRules(source: Vector[Symbol], target: Vector[Symbol], alignment: Alignment, spans: Stream[(Span, Span)]): Stream[(RulePair)] = {
    spans match {
      case head #:: tail =>
        Stream.cons(transformSpan(source, target, alignment, head), spansToRules(source, target, alignment, tail))
      case Stream.Empty =>
        Stream.Empty
    }
  }

  def extract(options: ExtractOptions)(sourceString: String, targetString: String, alignmentString: String): Stream[RulePair] = {
    val source = toSymbols((sourceString))
    val target = toSymbols(targetString)
    val alignment = new Alignment(alignmentString)
    val (parents, children, other) = extractPhrasePairs(options)(source, target, alignment)
    //println(children.map { _._1.toString }.toList.sorted.mkString("\n"))
    //println(parents.map { _.toString }.toList.sorted.mkString("\n"))
    val deduper = new scala.collection.mutable.HashSet[Span]
    val extractor = extractNT(options, children, deduper)_
    val rules1NT = extractor(parents.toList, _ => true).flatten
    val rules2NT = extractor(rules1NT.toList, filterPassNonTerminalRule(options)_).flatten
    //println(rules2NT.map { span => transformSpan(source, target, alignment, span).toString + span.toString }.toList.sorted.mkString("\n"))
    val phrases = (parents ++ other)
      .filter { case (srcSpan, _) => srcSpan.end - srcSpan.start + 1 <= options.maxSourcePhrase }
    //println(rules1NT.map { span => transformSpan(source, target, alignment, span).toString + span.toString }.toList.sorted.mkString("\n"))
    spansToRules(source, target, alignment, phrases.toStream #::: (rules1NT).filter(filterPassNonTerminalRule(options)_) #::: rules2NT)
  }

}