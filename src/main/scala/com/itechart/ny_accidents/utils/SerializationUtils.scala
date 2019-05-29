package com.itechart.ny_accidents.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64


object SerializationUtils {
  private final val ENCODING = "UTF-8"

  def serialize(value: Any): String = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    new String(
      Base64.getEncoder.encode(stream.toByteArray),
      ENCODING
    )
  }

  def deserialize[A](str: String): A = {
    val bytes = Base64.getDecoder.decode(str.getBytes(ENCODING))
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value: A = ois.readObject.asInstanceOf[A]
    ois.close()
    value
  }
}
