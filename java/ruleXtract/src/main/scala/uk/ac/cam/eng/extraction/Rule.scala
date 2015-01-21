package uk.ac.cam.eng.extraction

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.math.Ordering.Implicits.seqDerivedOrdering
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableUtils
import org.apache.hadoop.io.WritableComparable
import collection.JavaConversions._
import uk.ac.cam.eng.rule.retrieval.SidePattern

class WritableArrayBuffer extends ArrayBuffer[Symbol] with Writable {

  def readFields(in: java.io.DataInput): Unit = {
    clear();
    for (i <- 0 until WritableUtils.readVInt(in))
      this += Symbol.deserialise(WritableUtils.readVInt(in))
  }

  def write(out: java.io.DataOutput): Unit = {
    WritableUtils.writeVInt(out, size)
    for (s <- this) WritableUtils.writeVInt(out, s.serialised)
  }

  // Methods needed for java

  def javaSize(): Int = size

  def set(other: WritableArrayBuffer) = {
    clear
    this ++= other
  }

  def add(other: Symbol) = this += other

  override def toString() = this.map(_.toString).mkString("_")

  def toPattern(): SidePattern = new SidePattern(this.map {
    case _: Terminal => "w"
    case nt          => nt.serialised.toString()
  })

  def getWords() = this.count {
    case t: Terminal => t.serialised > 0
    case _           => false
  }
}

class Rule(val source: WritableArrayBuffer, val target: WritableArrayBuffer) extends Equals
  with Writable with WritableComparable[Rule] {

  def this() = this(new WritableArrayBuffer, new WritableArrayBuffer)

  def this(str: String) = {
    this()
    val parsed = str.split(" ").map(_.split("_").map(Symbol.deserialise(_)))
    if (parsed.size != 2) throw new Exception("Bad format for rule: " + str)
    source ++= parsed(0)
    target ++= parsed(1)
  }

  def this(other: Rule) =
    this(new WritableArrayBuffer ++= other.source, new WritableArrayBuffer ++= other.target)

  def this(src: java.util.List[Symbol], trg: java.util.List[Symbol]) =
    this(new WritableArrayBuffer ++= src, new WritableArrayBuffer ++= trg)

  override def toString() = source.toString() + " " + target.toString()

  def hasTwoNT() = source.contains(X1)

  def hasOneNT() = source.contains(X)

  def isPhrase() = !hasOneNT && !hasTwoNT

  private def isSwappingString(str: Seq[Symbol]): Boolean = {
    for (symbol <- str)
      if (symbol == X1) return false
      else if (symbol == X2) return true
    false
  }

  def isSwapping() = isSwappingString(source) || isSwappingString(target)

  def invertString(str: Seq[Symbol]) = {
    val results = new WritableArrayBuffer
    for (symbol <- str)
      if (symbol == X1) results += X2
      else if (symbol == X2) results += X1
      else results += symbol
    results
  }

  def invertNonTerminals(): Rule = new Rule(invertString(source), invertString(target))

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

  def readFields(in: java.io.DataInput): Unit = {
    source.readFields(in);
    target.readFields(in)
  }

  def write(out: java.io.DataOutput): Unit = {
    source.write(out)
    target.write(out)
  }

  override def compareTo(other: Rule) = S2TOrdering.compare(this, other)

  def getSource(): java.util.List[Symbol] = source

  def getTarget(): java.util.List[Symbol] = target

  private def set(str: WritableArrayBuffer, other: WritableArrayBuffer) = {
    str.clear()
    str ++= other
  }

  def setSource(other: WritableArrayBuffer) = set(source, other)

  def setTarget(other: WritableArrayBuffer) = set(target, other)

}

object S2TOrdering extends Ordering[Rule] {
  val vecOrdering = seqDerivedOrdering[ArrayBuffer, Symbol]

  override def compare(r1: Rule, r2: Rule) = {
    val diff = vecOrdering.compare(r1.source, r2.source)
    if (diff == 0) {
      vecOrdering.compare(r1.target, r2.target)
    } else {
      diff
    }
  }

}