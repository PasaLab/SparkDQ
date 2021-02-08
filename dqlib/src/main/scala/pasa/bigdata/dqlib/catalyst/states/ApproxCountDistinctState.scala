package pasa.bigdata.dqlib.catalyst.states

import java.lang.{Long => JLong}
import java.nio.ByteBuffer

object ApproxCountDistinctState {

  val NUM_WORDS = 52
  val RELATIVE_SD = 0.05

  val P: Int = Math.ceil(2.0d * Math.log(1.106d / RELATIVE_SD) / Math.log(2.0d)).toInt

  val IDX_SHIFT: Int = JLong.SIZE - P
  val W_PADDING: Long = 1L << (P - 1)
  val M: Int = 1 << P

  def wordsToBytes(words: Array[Long]): Array[Byte] = {
    require(words.length == NUM_WORDS)
    val bytes = Array.ofDim[Byte](NUM_WORDS * JLong.SIZE)
    val buffer = ByteBuffer.wrap(bytes).asLongBuffer()
    words.foreach { word => buffer.put(word) }
    bytes
  }

}
