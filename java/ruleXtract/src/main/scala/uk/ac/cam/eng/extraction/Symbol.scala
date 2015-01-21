package uk.ac.cam.eng.extraction

sealed abstract class Symbol(val serialised: Int) extends Ordered[Symbol] {

  def compare(that: Symbol) = this.serialised - that.serialised
}

object Symbol {
  def deserialise(symbol: Int): Symbol = {
    symbol match {
      case X.serialised  => X
      case X1.serialised => X1
      case X2.serialised => X2
      case S.serialised  => S
      case V.serialised  => V
      case _             => Terminal.create(symbol)
    }
  }

  def deserialise(symbol: String): Symbol = {
    if (symbol == X.toString) X
    else if (symbol == X1.toString) X1
    else if (symbol == X2.toString) X2
    else if (symbol == S.toString) S
    else if (symbol == V.toString) V
    else Terminal.create(symbol.toInt)
  }
}

case class Terminal private (token: Int) extends Symbol(token) {

  override def toString() = token.toString()

}

object Terminal {

  val terminalCache = for (i <- 0 until 10000) yield new Terminal(i)

  def create(token: Int) =
    if (token < 10000) terminalCache(token) else new Terminal(token)
}

case object X extends Symbol(-1)

case object X1 extends Symbol(-2)

case object X2 extends Symbol(-3)

case object S extends Symbol(-4)

case object V extends Symbol(-5)