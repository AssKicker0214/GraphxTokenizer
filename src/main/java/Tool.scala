import java.nio.ByteBuffer

/**
  * author: Qiao Hongbo
  * time: {$time}
  **/
object Tool {
  def utf8ToLong(singleCharacter: String): Long = {
    var bytes: Array[Byte] = new Array[Byte](8)
//    var bi = 0
//    for(bi <- 0 until 8){
//      bytes(bi) = 0
//    }
    val characterBytes = singleCharacter.getBytes("utf-8")
//    println(singleCharacter++characterBytes)
    var i = characterBytes.length-1

    while(i>=0){
      bytes(7-i) = characterBytes(i)
      i = i - 1
    }
//    println("byte buffer"++singleCharacter++ByteBuffer.wrap(bytes).array())
    val fig = ByteBuffer.wrap(bytes).getLong

    fig
  }

  def main(args: Array[String]): Unit = {
    val figure: Long = utf8ToLong("数")
    val figure2: Long = utf8ToLong("啰")

    println(figure)
    println(figure2)
  }
}
