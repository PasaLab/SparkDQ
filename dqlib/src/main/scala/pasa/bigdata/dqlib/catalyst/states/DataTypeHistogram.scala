package pasa.bigdata.dqlib.catalyst.states

import java.nio.ByteBuffer

case class DataTypeHistogram(numNull: Long, numFractional: Long, numIntegral: Long, numBoolean: Long, numString: Long) {

}

object DataTypeHistogram {

  val SIZE_IN_BYTES = 40

  def toBytes(numNull: Long, numFractional: Long, numIntegral: Long, numBoolean: Long,
              numString: Long): Array[Byte] = {
    val out = ByteBuffer.allocate(SIZE_IN_BYTES)
    val outB = out.asLongBuffer()

    outB.put(numNull)
    outB.put(numFractional)
    outB.put(numIntegral)
    outB.put(numBoolean)
    outB.put(numString)

    val bytes = new Array[Byte](out.remaining)
    out.get(bytes)
    bytes
  }
}

