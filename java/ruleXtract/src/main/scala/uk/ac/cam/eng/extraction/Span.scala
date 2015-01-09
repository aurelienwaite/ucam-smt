package uk.ac.cam.eng.extraction

sealed trait Phrase {
  def start: Int
  def end: Int
}

sealed trait oneNT {
  def startX: Int
  def endX: Int
}

sealed abstract class Span extends Phrase {
  def createSpan(span : Span) : Span
}

case class PhraseSpan(override val start: Int, override val end: Int) extends Span{
  override def createSpan(other : Span) : Span = {
    OneNTSpan(start, end, other.start, other.end)
  }
}

case class OneNTSpan(override val start: Int, override val end: Int,
                     override val startX: Int, override val endX: Int) extends Span with oneNT {
    override def createSpan(other : Span) : Span = {
    if(other.start < startX)
      TwoNTSpan(start, end, other.start, other.end, startX, endX)
     else
      TwoNTSpan(start, end, startX, endX, other.start, other.end)
  }
}

case class TwoNTSpan(override val start: Int, override val end: Int,
                     override val startX: Int, override val endX: Int, startX2: Int, endX2: Int)
  extends Span with oneNT{
    override def createSpan(other : Span) : Span = {
    throw new UnsupportedOperationException()
  }
}