package uk.ac.cam.eng.extraction

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.ListBuffer
import collection.JavaConversions._
import uk.ac.cam.eng.util.Pair

case class ExtractOptions(maxSourcePhrase: Int, maxSourceElements: Int, maxTerminalLength: Int, maxNonTerminalSpan: Int, removeMonotonicRepeats: Boolean)

object Extract {

  def toSymbols(s: String): Vector[Symbol] = {
    s.split(" ").map(x => Terminal.create(x.toInt)).toVector
  }

  def splitPhrasePairs(results: Seq[(Span, Span)], a: Alignment, options: ExtractOptions): (Seq[(Span, Span)], Seq[(Span, Span)], Seq[(Span, Span)]) = {
    val parentPhrases = new ArrayBuffer[(Span, Span)]
    val childPhrases = new ArrayBuffer[(Span, Span)]
    val otherPhrases = new ArrayBuffer[(Span, Span)]
    for (pair <- results) {
      pair match {
        case (srcSpan, trgSpan) => {
          val isStart = srcSpan.start == 0
          if ((!isStart || (isStart && a.isSourceAligned(0))) &&
          //a.isSourceAligned(srcSpan.end) &&
            a.isTargetAligned(trgSpan.end) && a.isTargetAligned(trgSpan.start)) {
            parentPhrases += pair
            var srcAligned = true
            for (i <- srcSpan.start to srcSpan.end if srcAligned) {
              srcAligned &= a.isSourceAligned(i)
            }
            if (srcAligned && srcSpan.end - srcSpan.start + 1 <= options.maxNonTerminalSpan)
              childPhrases += pair
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

  def createTwoNTSpan(oneNT: OneNTSpan, other: Span): TwoNTSpan =
    if (other.start < oneNT.startX)
      TwoNTSpan(oneNT.start, oneNT.end, other.start, other.end, oneNT.startX, oneNT.endX)
    else
      TwoNTSpan(oneNT.start, oneNT.end, oneNT.startX, oneNT.endX, other.start, other.end)

  def filterPassNonTerminalRule(opts: ExtractOptions, toFilter: Span): Boolean = {
    val (terminalLength, sourceElements) = toFilter match {
      case toFilter: OneNTSpan => {
        val terminalLength = toFilter.startX - toFilter.start + toFilter.end - toFilter.endX
        (terminalLength, terminalLength + 1)
      }
      case (toFilter: TwoNTSpan) => {
        val terminalLength = toFilter.startX - toFilter.start + toFilter.startX2 - 1 - toFilter.endX +
          toFilter.end - toFilter.endX2
        (terminalLength, terminalLength + 2)
      }
      case _ => throw new UnsupportedOperationException("Can only filter rules")
    }
    return sourceElements <= opts.maxSourceElements && terminalLength <= opts.maxTerminalLength
  }

  def constituent(parent: (Span, Span), child: (Span, Span), options: ExtractOptions): Boolean = {
    val (cSrcSpan, cTrgSpan) = child
    val (pSrcSpan, pTrgSpan) = parent
    val inRange = cSrcSpan.start >= pSrcSpan.start && cSrcSpan.end <= pSrcSpan.end
    parent match {
      case (pSrcSpan: OneNTSpan, pTrgSpan: OneNTSpan) => {
        val inRangeNT = inRange & (cSrcSpan.end < pSrcSpan.startX || cSrcSpan.start > pSrcSpan.endX)
        val hasConsecutiveNT =
          (cSrcSpan.end + 1 == pSrcSpan.startX || pSrcSpan.endX + 1 == cSrcSpan.start ||
            cTrgSpan.end + 1 == pTrgSpan.startX || pTrgSpan.endX + 1 == cTrgSpan.start)
        inRangeNT && !hasConsecutiveNT
      }
      case _ => {
        val notSameSpan = (pSrcSpan.start != cSrcSpan.start || pSrcSpan.end != cSrcSpan.end) &&
          (pTrgSpan.start != cTrgSpan.start || pTrgSpan.end != cTrgSpan.end)
        notSameSpan && inRange
      }
    }
  }

  def deduperKey(srcSpan: OneNTSpan): (OneNTSpan, Boolean) =
    if (srcSpan.start == srcSpan.startX)
      Tuple2(OneNTSpan(srcSpan.endX, srcSpan.end, srcSpan.endX, srcSpan.endX), true)
    else if (srcSpan.end == srcSpan.endX)
      Tuple2(OneNTSpan(srcSpan.start, srcSpan.startX, srcSpan.startX, srcSpan.startX), true)
    else
      (srcSpan, false)

  def deduperKey(srcSpan: TwoNTSpan): (TwoNTSpan, Boolean) =
    if (srcSpan.start == srcSpan.startX && srcSpan.end == srcSpan.endX2) {
      Tuple2(TwoNTSpan(srcSpan.endX, srcSpan.startX2, srcSpan.endX,
        srcSpan.endX, srcSpan.startX2, srcSpan.startX2), true)
    } else
      (srcSpan, false)

  def add(opts: ExtractOptions, results: Buffer[(Span, Span)], srcSpan: Span, trgSpan: Span,
          dedupe: Boolean, deduper: scala.collection.mutable.HashSet[Span]): Unit =
    if (filterPassNonTerminalRule(opts, srcSpan) && (if (dedupe && opts.removeMonotonicRepeats) deduper add srcSpan else true))
      results += Tuple2(srcSpan, trgSpan)

  def extractNT(children: Seq[(Span, Span)], toExtract: (Span, Span),
                results: Buffer[(Span, Span)], options: ExtractOptions,
                deduper: scala.collection.mutable.HashSet[Span]): Unit = {
    val constituentPhrases = children.filter(constituent(toExtract, _, options))
    val (srcSpan, trgSpan) = toExtract
    for (i <- 0 until constituentPhrases.size) {
      val (srcOuter, trgOuter) = constituentPhrases(i)
      val (src1NT, dedupe) =
        deduperKey(OneNTSpan(srcSpan.start, srcSpan.end, srcOuter.start, srcOuter.end))
      val trg1NT = OneNTSpan(trgSpan.start, trgSpan.end, trgOuter.start, trgOuter.end)
      add(options, results, src1NT, trg1NT, dedupe, deduper)
      for (j <- i + 1 until constituentPhrases.size) {
        if (constituent((src1NT, trg1NT), constituentPhrases(j), options)) {
          val (srcInner, trgInner) = constituentPhrases(j)
          val (src2NT, dedupe) = deduperKey(createTwoNTSpan(src1NT, srcInner))
          val trg2NT = createTwoNTSpan(trg1NT, trgInner)
          add(options, results, src2NT, trg2NT, dedupe, deduper)
        }
      }
    }
  }

  def extractNT(options: ExtractOptions, children: Seq[(Span, Span)],
                deduper: scala.collection.mutable.HashSet[Span])(toExtract: Seq[(Span, Span)]): Seq[(Span, Span)] = {
    val results = new ListBuffer[(Span, Span)]
    for (parent <- toExtract) {
      extractNT(children, parent, results, options, deduper)
    }
    results
  }

  def transformSpan(source: Vector[Symbol], target: Vector[Symbol], alignment: Alignment, span: (Span, Span)): RulePair = {
    var phraseAlignment = alignment.extractPhraseAlignment(span)
    span match {
      case (src: PhraseSpan, trg: PhraseSpan) => {
        val r = new Rule(new RuleString ++= source.slice(src.start, src.end + 1),
          new RuleString ++= target.slice(trg.start, trg.end + 1))
        (r, phraseAlignment)
      }
      case (src: OneNTSpan, trg: OneNTSpan) => {
        val slicer = (symbols: Vector[Symbol], span: OneNTSpan) => new RuleString ++=
          symbols.slice(span.start, span.startX) += X ++= symbols.slice(span.endX + 1, span.end + 1)
        (new Rule(slicer(source, src), slicer(target, trg)), phraseAlignment)
      }
      case (src: TwoNTSpan, trg: TwoNTSpan) => {
        val slicer = (symbols: Vector[Symbol], span: TwoNTSpan) => new RuleString ++=
          symbols.slice(span.start, span.startX) += X1 ++= symbols.slice(span.endX + 1, span.startX2) += X2 ++=
          symbols.slice(span.endX2 + 1, span.end + 1)
        val trgString = if (trg.startX < trg.startX2) slicer(target, trg)
        else new RuleString ++= target.slice(trg.start, trg.startX2) +=
          X2 ++= target.slice(trg.endX2 + 1, trg.startX) += X1 ++=
          target.slice(trg.endX + 1, trg.end + 1)
        (new Rule(slicer(source, src), trgString), phraseAlignment)
      }
      case _ => throw new UnsupportedOperationException("Must use spans of the same type")
    }
  }

  def extract(options: ExtractOptions)(sourceString: String, targetString: String, alignmentString: String): Seq[RulePair] = {
    val source = toSymbols((sourceString))
    val target = toSymbols(targetString)
    val alignment = new Alignment(alignmentString)
    val (parents, children, other) = extractPhrasePairs(options)(source, target, alignment)
    val deduper = new scala.collection.mutable.HashSet[Span]
    val all = parents ++ other
    val extractor = extractNT(options, children, deduper)_
    val rules = extractor(parents)
    val phrases = (parents ++ other)
      .filter { case (srcSpan, _) => srcSpan.end - srcSpan.start + 1 <= options.maxSourcePhrase }
    (phrases ++ rules).map(transformSpan(source, target, alignment, _))
  }

  def extractJava(options: ExtractOptions)(sourceString: String, targetString: String, alignmentString: String): java.util.List[Pair[Rule, Alignment]] =
    extract(options)(sourceString, targetString, alignmentString).map { ra =>
      val (r, a) = ra
      new Pair(r, a)
    }
}