package uk.ac.cam.eng.extraction

sealed abstract class Symbol(val serialised: Int) extends Ordered[Symbol] {

  def compare(that: Symbol) = this.serialised - that.serialised

}

object Symbol {

  val mapping = Map(V.toString() -> V, V1.toString() -> V1,
    S.toString() -> S, X.toString() -> X, D.toString -> D, 
    oov.toString() -> oov, dr.toString() -> dr)

  def deserialise(symbol: Int): Symbol = {
    symbol match {
      case V.serialised  => V
      case V1.serialised => V1
      case S.serialised  => S
      case X.serialised  => X
      case D.serialised  => D
      case oov.serialised  => oov
      case dr.serialised  => dr
      case _             => Terminal.create(symbol)
    }
  }

  def deserialise(symbol: String): Symbol = 
    mapping.getOrElse(symbol, Terminal.create(symbol.toInt))
}

case class Terminal private (token: Int) extends Symbol(token) {

  override def toString() = token.toString()

}

object Terminal {

  val terminalCache = for (i <- 0 until 10000) yield new Terminal(i)

  def create(token: Int) =
    if (token < 10000) terminalCache(token) else new Terminal(token)
}

case object V extends Symbol(-1)

case object V1 extends Symbol(-2)

case object S extends Symbol(-3)

case object X extends Symbol(-4)

case object D extends Symbol(-5)

case object oov extends Symbol(-6){
  override def toString() = "<oov>"
}

case object dr extends Symbol(-7){
  override def toString() = "<dr>"
}