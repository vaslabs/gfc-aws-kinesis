package com.gilt.gfc.avro.util

import java.nio.ByteBuffer

object ByteBufferUtil {

  def toByteArray( bb: ByteBuffer
                 ): Array[Byte] = {

    val bytes = Array.ofDim[Byte](bb.remaining())
    bb.get(bytes)
    bytes
  }

  /** Avro APIs work with byte arrays, some IO we interface works with ByteBuffers,
    * if they are already array-backed we can avoid extra conversion to byte array,
    * otherwise - convert.
    *
    * @return ByteBuffer that is guaranteed to be array backed
    */
  def toArrayBackedByteBuffer( bb: ByteBuffer
                             ): ByteBuffer = {

    if (bb.hasArray) {
      bb
    } else {
      ByteBuffer.wrap(toByteArray(bb))
    }
  }

  /** For debugging, hex dump of a byte array. */
  def toHexString( ba: Array[Byte]
                 ): String = {

    toHexString(ByteBuffer.wrap(ba))
  }

  /** For debugging, hex dump of a byte buffer. */
  def toHexString( bb: ByteBuffer
                 ): String = {

    val sb = new StringBuilder( bb.remaining * 2 )

    while ( bb.hasRemaining ) {
      sb.append("%02X".format( bb.get & 0xff ))
    }

    sb.toString
  }
}
