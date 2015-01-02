package uk.ac.cam.eng.extraction

// first import all necessary types from package `collection.mutable`
import collection.mutable.{ HashMap, SortedSet, TreeSet }

class Alignment {

  val s2t = new HashMap[Int, SortedSet[Int]]
  val t2s = new HashMap[Int, SortedSet[Int]]

  def this(alignmentString: String) = {
    this()
    val alignments = alignmentString.split(" ") map (x => x split ("-") map (_.toInt))
    for (alignment <- alignments) {
      addAlignment(alignment(0), alignment(1))
    }
  }

  def this(other: Alignment) = {
    this()
    other.s2t.foreach {
      case (key, value) => value.foreach { addAlignment(key, _) }
    }
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

  def addAlignment(sourceIndex: Int, targetIndex: Int) {
    s2t.getOrElseUpdate(sourceIndex, new TreeSet[Int]).add(targetIndex)
    t2s.getOrElseUpdate(targetIndex, new TreeSet[Int]).add(sourceIndex)
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
    prime * (prime + s2t.hashCode ) + t2s.hashCode
  }

}