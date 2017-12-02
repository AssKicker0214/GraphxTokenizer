import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.collection.mutable.ArrayBuffer

object GraphxCreator extends App {
  val sparkConf = new SparkConf()
    .setAppName("GraphxTokenizer")
    .setMaster("local[2]")
    .set("spark.driver.host", "localhost")
    // 从数据库读取句子（生文本）
    .set("spark.mongodb.input.uri", "mongodb://" + Configuration.MONGODB_HOST + "/chinese.train")
    // 经过计算，将边（起点，权重，终点）存入train_edges
    .set("spark.mongodb.output.uri", "mongodb://" + Configuration.MONGODB_HOST + "/chinese.train_edges")
  val sc: SparkContext = new SparkContext(sparkConf)

  def makeEdge(): Unit = {
    def getCharacterPair(content: String): Array[List[String]] = {
      var characterArray = content.split("")
      var edgeArray = new ArrayBuffer[List[String]]()

      def filterCharacter(character: String): Boolean = {
        val punctuations = Array[String]("。", "，", "！", "？", "：", "；", "～", "（", "）", " ", "~", "?", ";", ".", "&",
          "\0", "\'", "(", ")", "[", "]", ",", "\\", "$", "@", "/", "?")
        for (punctuation <- punctuations) {
          if (character.equals(punctuation) ||
            (character.compareToIgnoreCase("0") >= 0 && character.compareToIgnoreCase("9") <= 0)) {
            return false
          }
        }
        true
      }

      var i = 0
      for (i <- 0 until characterArray.length - 1) {
        val character1 = characterArray(i)
        val character2 = characterArray(i + 1)
        if (filterCharacter(character1) && filterCharacter(character2)) {
          edgeArray += List[String](character1, character2)
        }
      }
      edgeArray.toArray[List[String]]
    }

    val textsRdd = MongoSpark.load(sc)
    val contentRdd = textsRdd.map(doc => doc.get("text").asInstanceOf[String])
    val pairRdd: RDD[List[String]] = contentRdd.flatMap(getCharacterPair)
    val pairCountRdd = pairRdd.map(pair => (pair, 1))
    val edgeWeightRdd: RDD[(List[String], Int)] = pairCountRdd.reduceByKey((x1, x2) => x1 + x2)

    val writeConfig = WriteConfig(Map("collection" -> "train_edges"), Some(WriteConfig(sc)))

    val documents = edgeWeightRdd.map(pair =>
      Document.parse(s"{src_name:'${pair._1.head}',des_name:'${pair._1(1)}',weight:${pair._2}}"))

    MongoSpark.save(documents, writeConfig)
  }

  //  MongoSpark
  println("start")

  makeEdge()
}