package pe.com.belcorp.datalake.utils

import java.time.{Clock, Instant}
import java.util.function.Supplier

/**
  * Random utility methods with nowhere better to go
  */
object Goodies {
  def timeIt(fun: => Any): Double = {
    val start = System.nanoTime()

    fun

    (System.nanoTime() - start) / 1000000d
  }

  def logIt(msg: Any): Unit = {
    System.err.println(s"[NOTERR] $msg")
    println(s"[INFO] $msg")
  }
  
  private object clockFactory extends Supplier[Clock] {
    override def get(): Clock = Clock.systemUTC()
  }

  private var _clocks = ThreadLocal.withInitial(clockFactory)

  def now: Instant = Instant.now(clock)
  def clock: Clock = _clocks.get()
  def withClock[T](newClock: Clock, f: Clock => T): T = {
    val oldClock = _clocks.get()
    try {
      _clocks.set(newClock)
      f(newClock)
    } finally {
      _clocks.set(oldClock)
    }
  }
}
