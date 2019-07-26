package com.itechart.accidents.utils

import com.itechart.accidents.entity.{Accident, MergedData}
import org.scalatest.FunSpec

class SerializationUtilsTest extends FunSpec {
  describe("A SerializationUtils") {
    it("should serialize MergedData") {
      val testData = MergedData(
        Accident(Some(1), None, None, None, None,
          None, None, None, None, 0, 0, 0,
          0, 0, 0, 0, 0, List(), List()), None, None)

      val expectedValue = "rO0ABXNyAChjb20uaXRlY2hhcnQuYWNjaWRlbnRzLmVudGl0eS5NZXJnZWREYXRhoyD1uvw9g6UCAANMAAhhY2NpZGVudHQAKExjb20vaXRlY2hhcnQvYWNjaWRlbnRzL2VudGl0eS9BY2NpZGVudDtMAAhkaXN0cmljdHQADkxzY2FsYS9PcHRpb247TAAHd2VhdGhlcnEAfgACeHBzcgAmY29tLml0ZWNoYXJ0LmFjY2lkZW50cy5lbnRpdHkuQWNjaWRlbnQLBQRLgcrQowIAE0kADmN5Y2xpc3RJbmp1cmVkSQANY3ljbGlzdEtpbGxlZEkAD21vdG9yaXN0SW5qdXJlZEkADm1vdG9yaXN0S2lsbGVkSQAScGVkZXN0cmlhbnNJbmp1cmVkSQARcGVkZXN0cmlhbnNLaWxsZWRJAA5wZXJzb25zSW5qdXJlZEkADXBlcnNvbnNLaWxsZWRMAAdib3JvdWdocQB+AAJMABNjb250cmlidXRpbmdGYWN0b3JzdAAhTHNjYWxhL2NvbGxlY3Rpb24vaW1tdXRhYmxlL0xpc3Q7TAALY3Jvc3NTdHJlZXRxAH4AAkwADmRhdGVUaW1lTWlsbGlzcQB+AAJMAAhsYXRpdHVkZXEAfgACTAANbG9jYWxEYXRlVGltZXEAfgACTAAJbG9uZ2l0dWRlcQB+AAJMAAlvZmZTdHJlZXRxAH4AAkwACG9uU3RyZWV0cQB+AAJMAAl1bmlxdWVLZXlxAH4AAkwAC3ZlaGljbGVUeXBlcQB+AAV4cAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHNyADJzY2FsYS5jb2xsZWN0aW9uLmltbXV0YWJsZS5MaXN0JFNlcmlhbGl6YXRpb25Qcm94eQAAAAAAAAABAwAAeHBzcgAsc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuTGlzdFNlcmlhbGl6ZUVuZCSKXGNb91MLbQIAAHhweHEAfgAJcQB+AAlxAH4ACXEAfgAJcQB+AAlxAH4ACXEAfgAJc3IACnNjYWxhLlNvbWURIvJpXqGLdAIAAUwABXZhbHVldAASTGphdmEvbGFuZy9PYmplY3Q7eHEAfgAIc3IADmphdmEubGFuZy5Mb25nO4vkkMyPI98CAAFKAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAAAAAABcQB+AAtxAH4ACXEAfgAJ"


      println(SerializationUtils.serialize(testData))
      assert(SerializationUtils.serialize(testData) == expectedValue)
    }

    it("should deserialize serialized string") {
      val testData = "rO0ABXNyAChjb20uaXRlY2hhcnQuYWNjaWRlbnRzLmVudGl0eS5NZXJnZWREYXRhoyD1uvw9g6UCAANMAAhhY2NpZGVudHQAKExjb20vaXRlY2hhcnQvYWNjaWRlbnRzL2VudGl0eS9BY2NpZGVudDtMAAhkaXN0cmljdHQADkxzY2FsYS9PcHRpb247TAAHd2VhdGhlcnEAfgACeHBzcgAmY29tLml0ZWNoYXJ0LmFjY2lkZW50cy5lbnRpdHkuQWNjaWRlbnQLBQRLgcrQowIAE0kADmN5Y2xpc3RJbmp1cmVkSQANY3ljbGlzdEtpbGxlZEkAD21vdG9yaXN0SW5qdXJlZEkADm1vdG9yaXN0S2lsbGVkSQAScGVkZXN0cmlhbnNJbmp1cmVkSQARcGVkZXN0cmlhbnNLaWxsZWRJAA5wZXJzb25zSW5qdXJlZEkADXBlcnNvbnNLaWxsZWRMAAdib3JvdWdocQB+AAJMABNjb250cmlidXRpbmdGYWN0b3JzdAAhTHNjYWxhL2NvbGxlY3Rpb24vaW1tdXRhYmxlL0xpc3Q7TAALY3Jvc3NTdHJlZXRxAH4AAkwADmRhdGVUaW1lTWlsbGlzcQB+AAJMAAhsYXRpdHVkZXEAfgACTAANbG9jYWxEYXRlVGltZXEAfgACTAAJbG9uZ2l0dWRlcQB+AAJMAAlvZmZTdHJlZXRxAH4AAkwACG9uU3RyZWV0cQB+AAJMAAl1bmlxdWVLZXlxAH4AAkwAC3ZlaGljbGVUeXBlcQB+AAV4cAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHNyADJzY2FsYS5jb2xsZWN0aW9uLmltbXV0YWJsZS5MaXN0JFNlcmlhbGl6YXRpb25Qcm94eQAAAAAAAAABAwAAeHBzcgAsc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuTGlzdFNlcmlhbGl6ZUVuZCSKXGNb91MLbQIAAHhweHEAfgAJcQB+AAlxAH4ACXEAfgAJcQB+AAlxAH4ACXEAfgAJc3IACnNjYWxhLlNvbWURIvJpXqGLdAIAAUwABXZhbHVldAASTGphdmEvbGFuZy9PYmplY3Q7eHEAfgAIc3IADmphdmEubGFuZy5Mb25nO4vkkMyPI98CAAFKAAV2YWx1ZXhyABBqYXZhLmxhbmcuTnVtYmVyhqyVHQuU4IsCAAB4cAAAAAAAAAABcQB+AAtxAH4ACXEAfgAJ"
      val expectedValue = MergedData(
        Accident(Some(1), None, None, None, None,
          None, None, None, None, 0, 0, 0,
          0, 0, 0, 0, 0, List(), List()), None, None)
      val result = SerializationUtils.deserialize[MergedData](testData)

      assert(result == expectedValue)
    }
  }
}