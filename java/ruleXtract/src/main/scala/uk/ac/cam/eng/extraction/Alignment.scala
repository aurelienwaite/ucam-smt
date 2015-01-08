package uk.ac.cam.eng.extraction

import collection.mutable.{ HashMap, SortedSet, TreeSet }
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

class Alignment {

  val s2t = new HashMap[Int, ArrayBuffer[Int]]
  val t2s = new HashMap[Int, ArrayBuffer[Int]]

  def this(alignmentString: String) = {
    this()
    val alignments = alignmentString.split(" ") map (x => x split ("-") map (_.toInt))
    for (alignment <- alignments) {
      addAlignment(alignment(0), alignment(1))
    }
    sortAlignments()
  }

  def this(other: Alignment) = {
    this()
    for((key, value) <- other.s2t)
      s2t(key) = new ArrayBuffer[Int] ++= value
        for((key, value) <- other.t2s)
      t2s(key) = new ArrayBuffer[Int] ++= value
  }

  def getS2T(index: Int) = {
    s2t(index)
  }

  def getT2S(index: Int) = {
    t2s(index)
  }

  def isTargetAligned(index: Int): Boolean = {
    t2s.contains(index)
  }

  def isSourceAligned(index: Int): Boolean = {
    s2t.contains(index)
  }

  private def addAlignment(sourceIndex: Int, targetIndex: Int) {
    s2t.getOrElseUpdate(sourceIndex, new ArrayBuffer) += targetIndex
    t2s.getOrElseUpdate(targetIndex, new ArrayBuffer) += sourceIndex
  }

  private def sortAlignments(): Alignment = {
    for ((k, a) <- s2t) s2t(k) = a.sortWith(_ < _)
    for ((k, a) <- t2s) t2s(k) = a.sortWith(_ < _)
    this
  }

  def clear() = {
    s2t.clear()
    t2s.clear()
  }

  override def toString(): String = {
    val buff = new StringBuffer()
    s2t.foreach {
      case (key, value) => {
        for (t <- value) {
          buff.append(key).append("-").append(t).append(" ")
        }
      }
    }
    buff.toString
  }

  def incrementTrg(inc: Int): Alignment = {
    val decremented = new Alignment()
    s2t.foreach {
      case (key, value) => {
        value.foreach { t => decremented.addAlignment(key, t + inc) }
      }
    }
    return decremented
  }

  def canEqual(other: Any) = {
    other.isInstanceOf[uk.ac.cam.eng.extraction.Alignment]
  }

  override def equals(other: Any) = {
    other match {
      case that: uk.ac.cam.eng.extraction.Alignment => that.canEqual(Alignment.this) && s2t == that.s2t && t2s == that.t2s
      case _                                        => false
    }
  }

  override def hashCode() = {
    val prime = 41
    prime * (prime + s2t.hashCode) + t2s.hashCode
  }

  def adjustExtractedAlignments(srcStartSpan: Int, srcEndSpan: Int, trgStartSpan: Int, trgEndSpan: Int, phrase: Rule): Alignment = {
    val adjusted = new Alignment()
    s2t.foreach {
      case (src, value) => value.foreach { trg =>
        if ((src < srcStartSpan || src > srcEndSpan) &&
          (trg < trgStartSpan || trg > trgEndSpan)) {
          val newSrc = if (src > srcStartSpan) src - phrase.source.length + 1 else src
          val newTrg = if (trg > trgStartSpan) trg - phrase.target.length + 1 else trg
          adjusted.s2t.getOrElseUpdate(newSrc, new ArrayBuffer) += newTrg
        }
      }
    }
    t2s.foreach {
      case (trg, value) => value.foreach { src =>
        if ((src < srcStartSpan || src > srcEndSpan) &&
          (trg < trgStartSpan || trg > trgEndSpan)) {
          val newSrc = if (src > srcStartSpan) src - phrase.source.length + 1 else src
          val newTrg = if (trg > trgStartSpan) trg - phrase.target.length + 1 else trg
          adjusted.t2s.getOrElseUpdate(newTrg, new ArrayBuffer) += newSrc
        }
      }
    }
    adjusted
  }
  
  def extractPhraseAlignment(srcStartSpan: Int, srcEndSpan: Int, trgStartSpan: Int) : Alignment ={
    val phraseAlignment = new Alignment
    s2t.foreach {
      case (src, value) => value.foreach { trg =>
        if(src>=srcStartSpan && src<=srcEndSpan) 
          phraseAlignment.addAlignment(src-srcStartSpan, trg-trgStartSpan)
      }
    }
    phraseAlignment.sortAlignments()
  }
  
  
}