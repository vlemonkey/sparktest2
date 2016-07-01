package com.test

/**
 * Created by s on 16-5-16.
 */
object SimpleTest {
  def main(args: Array[String]) {
    val c = Tuple3(1: Any, "a": Any, 4.4: Any)
    val d = c match {
      case (1, _, _ ) => 0
      case (x:Int, y:Int, z: Int) => 2
      case (x:Int, y:String, z: Double) => 1
      case (x:Int , y, z) if x > 1 => 3

    }
  }

}
