package uk.ac.cam.eng.extraction

sealed trait Phrase {
  def start: Int
  def end: Int
}

sealed trait oneNT {
  def startX: Int
  def endX: Int
}

sealed abstract class Span extends Phrase 

case class PhraseSpan(override val start: Int, override val end: Int) extends Span

case class OneNTSpan(override val start: Int, override val end: Int,
                     override val startX: Int, override val endX: Int) extends Span with oneNT

case class TwoNTSpan(override val start: Int, override val end: Int,
                     override val startX: Int, override val endX: Int, startX2: Int, endX2: Int)
  extends Span with oneNT
  