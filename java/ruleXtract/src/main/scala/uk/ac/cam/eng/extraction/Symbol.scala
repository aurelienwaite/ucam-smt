package uk.ac.cam.eng.extraction

sealed abstract class Symbol extends Ordered[Symbol]{
  def serialise(): Int
  
  def compare(that: Symbol) =  this.serialise - that.serialise
}

case class Terminal(token: Int) extends Symbol {
  override def toString() = token.toString()

  override def serialise() = token
}

case object X extends Symbol {
  override def serialise() = -1
}
case object X1 extends Symbol {
  override def serialise() = -2
}
case object X2 extends Symbol {
  override def serialise() = -3
}
case object S extends Symbol {
  override def serialise() = -4
}
case object V extends Symbol {
  override def serialise() = -5
}