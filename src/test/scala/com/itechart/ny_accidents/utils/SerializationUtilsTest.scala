package com.itechart.ny_accidents.utils

import com.itechart.ny_accidents.entity.{Accident, MergedData}
import org.scalatest.FunSpec

class SerializationUtilsTest extends FunSpec {
  describe("A SerializationUtils") {
    it("should serialize MergedData") {
      val testData = MergedData(
        Accident(Some(1), None, None, None, None,
          None, None, None, None, None, None, None,
          None, None, None, None, None, List(), List()),
        None,
        None
      )
      val expectedValue = "rO0ABXNyACtjb20uaXRlY2hhcnQubnlfYWNjaWRlbnRzLmVudGl0eS5NZXJnZWREYXRheFwKq74VmBUCAANMAAhhY2NpZGVudHQAK0xjb20vaXRlY2hhcnQvbnlfYWNjaWRlbnRzL2VudGl0eS9BY2NpZGVudDtMAAhkaXN0cmljdHQADkxzY2FsYS9PcHRpb247TAAHd2VhdGhlcnEAfgACeHBzcgApY29tLml0ZWNoYXJ0Lm55X2FjY2lkZW50cy5lbnRpdHkuQWNjaWRlbnTiR4PA54sWxAIAE0wAB2Jvcm91Z2hxAH4AAkwAE2NvbnRyaWJ1dGluZ0ZhY3RvcnN0ACFMc2NhbGEvY29sbGVjdGlvbi9pbW11dGFibGUvTGlzdDtMAAtjcm9zc1N0cmVldHEAfgACTAAOY3ljbGlzdEluanVyZWRxAH4AAkwADWN5Y2xpc3RLaWxsZWRxAH4AAkwACGRhdGVUaW1lcQB+AAJMAA5kYXRlVGltZU1pbGxpc3EAfgACTAAIbGF0aXR1ZGVxAH4AAkwACWxvbmdpdHVkZXEAfgACTAAPbW90b3Jpc3RJbmp1cmVkcQB+AAJMAA5tb3RvcmlzdEtpbGxlZHEAfgACTAAJb2ZmU3RyZWV0cQB+AAJMAAhvblN0cmVldHEAfgACTAAScGVkZXN0cmlhbnNJbmp1cmVkcQB+AAJMABFwZWRlc3RyaWFuc0tpbGxlZHEAfgACTAAOcGVyc29uc0luanVyZWRxAH4AAkwADXBlcnNvbnNLaWxsZWRxAH4AAkwACXVuaXF1ZUtleXEAfgACTAALdmVoaWNsZVR5cGVxAH4ABXhwc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHNyADJzY2FsYS5jb2xsZWN0aW9uLmltbXV0YWJsZS5MaXN0JFNlcmlhbGl6YXRpb25Qcm94eQAAAAAAAAABAwAAeHBzcgAsc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuTGlzdFNlcmlhbGl6ZUVuZCSKXGNb91MLbQIAAHhweHEAfgAJcQB+AAlxAH4ACXEAfgAJcQB+AAlxAH4ACXEAfgAJcQB+AAlxAH4ACXEAfgAJcQB+AAlxAH4ACXEAfgAJcQB+AAlxAH4ACXNyAApzY2FsYS5Tb21lESLyaV6hi3QCAAFMAAV2YWx1ZXQAEkxqYXZhL2xhbmcvT2JqZWN0O3hxAH4ACHNyAA5qYXZhLmxhbmcuTG9uZzuL5JDMjyPfAgABSgAFdmFsdWV4cgAQamF2YS5sYW5nLk51bWJlcoaslR0LlOCLAgAAeHAAAAAAAAAAAXEAfgALcQB+AAlxAH4ACQ=="

      assert(SerializationUtils.serialize(testData) == expectedValue)
    }

    it("should deserialize serialized string") {
      val testData = "rO0ABXNyACtjb20uaXRlY2hhcnQubnlfYWNjaWRlbnRzLmVudGl0eS5NZXJnZWREYXRheFwKq74VmBUCAANMAAhhY2NpZGVudHQAK0xjb20vaXRlY2hhcnQvbnlfYWNjaWRlbnRzL2VudGl0eS9BY2NpZGVudDtMAAhkaXN0cmljdHQADkxzY2FsYS9PcHRpb247TAAHd2VhdGhlcnEAfgACeHBzcgApY29tLml0ZWNoYXJ0Lm55X2FjY2lkZW50cy5lbnRpdHkuQWNjaWRlbnTiR4PA54sWxAIAE0wAB2Jvcm91Z2hxAH4AAkwAE2NvbnRyaWJ1dGluZ0ZhY3RvcnN0ACFMc2NhbGEvY29sbGVjdGlvbi9pbW11dGFibGUvTGlzdDtMAAtjcm9zc1N0cmVldHEAfgACTAAOY3ljbGlzdEluanVyZWRxAH4AAkwADWN5Y2xpc3RLaWxsZWRxAH4AAkwACGRhdGVUaW1lcQB+AAJMAA5kYXRlVGltZU1pbGxpc3EAfgACTAAIbGF0aXR1ZGVxAH4AAkwACWxvbmdpdHVkZXEAfgACTAAPbW90b3Jpc3RJbmp1cmVkcQB+AAJMAA5tb3RvcmlzdEtpbGxlZHEAfgACTAAJb2ZmU3RyZWV0cQB+AAJMAAhvblN0cmVldHEAfgACTAAScGVkZXN0cmlhbnNJbmp1cmVkcQB+AAJMABFwZWRlc3RyaWFuc0tpbGxlZHEAfgACTAAOcGVyc29uc0luanVyZWRxAH4AAkwADXBlcnNvbnNLaWxsZWRxAH4AAkwACXVuaXF1ZUtleXEAfgACTAALdmVoaWNsZVR5cGVxAH4ABXhwc3IAC3NjYWxhLk5vbmUkRlAk9lPKlKwCAAB4cgAMc2NhbGEuT3B0aW9u/mk3/dsOZnQCAAB4cHNyADJzY2FsYS5jb2xsZWN0aW9uLmltbXV0YWJsZS5MaXN0JFNlcmlhbGl6YXRpb25Qcm94eQAAAAAAAAABAwAAeHBzcgAsc2NhbGEuY29sbGVjdGlvbi5pbW11dGFibGUuTGlzdFNlcmlhbGl6ZUVuZCSKXGNb91MLbQIAAHhweHEAfgAJcQB+AAlxAH4ACXEAfgAJcQB+AAlxAH4ACXEAfgAJcQB+AAlxAH4ACXEAfgAJcQB+AAlxAH4ACXEAfgAJcQB+AAlxAH4ACXNyAApzY2FsYS5Tb21lESLyaV6hi3QCAAFMAAV2YWx1ZXQAEkxqYXZhL2xhbmcvT2JqZWN0O3hxAH4ACHNyAA5qYXZhLmxhbmcuTG9uZzuL5JDMjyPfAgABSgAFdmFsdWV4cgAQamF2YS5sYW5nLk51bWJlcoaslR0LlOCLAgAAeHAAAAAAAAAAAXEAfgALcQB+AAlxAH4ACQ=="
      val expectedValue = MergedData(
        Accident(Some(1), None, None, None, None,
          None, None, None, None, None, None, None,
          None, None, None, None, None, List(), List()),
        None,
        None
      )
      val result = SerializationUtils.deserialize[MergedData](testData)

      assert(result == expectedValue)
    }
  }
}