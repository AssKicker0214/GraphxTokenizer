import java.io._

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, MultiGraph, MultiNode}
import sun.security.provider.certpath.Vertex
import org.apache.log4j

// 这部分代码的风格问题已经批评过当事人了。。。。我懒得重构，因为太难看了
// 可以忽略，就是将分好词的图变成文本文件
object disgra {
  def cutAndSave(graph: Graph[(String, Char), Int]): Unit = {
    import org.apache.log4j.PropertyConfigurator
    PropertyConfigurator.configure("./log4j.properties")

//
    val tend = graph.vertices.mapValues((id, yu) => id).minus(graph.outDegrees.mapValues((id, yu) => id))
    val ttend = tend.map(tu => tu._1).collect()
//
    val tstart = graph.vertices.mapValues((id, yu) => id).minus(graph.inDegrees.mapValues((id, yu) => id))
    val ttstart = tstart.map(tu => tu._1).collect()

    val res = graph.mapVertices((id, yu) => {
      if (ttstart.contains(id)) {
        (yu._1, 'X')
      }
      else {
        (yu._1, yu._2)
      }
    }).pregel("", 120)(
      (id, ownmessage, resmes) => {
        if (resmes == "") {
          (resmes + ownmessage._1, ownmessage._2)
        }
        else {
          ownmessage._2 match {
            case 'M' => (resmes + ownmessage._1, 'X');
            case 'A' => (resmes + ownmessage._1, 'X');
            case 'E' => (resmes + ownmessage._1 + " ", 'X');
            case 'B' => (resmes + " " + ownmessage._1, 'X');
            case 'S' => (resmes + ownmessage._1 + " ", 'X');
            case 'I' => (resmes + ownmessage._1, 'X');
            case _ => (ownmessage._1, 'X')
          }
        }
      },
      triplet => {
        if (ttstart.contains(triplet.srcId) && triplet.srcAttr._2 != 'X' || triplet.srcAttr._2 == 'X' && triplet.dstAttr._2 != 'X') {
          Iterator((triplet.dstId, triplet.srcAttr._1))
        }
        else {
          Iterator.empty
        }
      },
      (a, b) => a + "::" + b)
    val resarray = res.vertices.filter(tu => ttend.contains(tu._1)).map(tu => tu._2._1).collect()
    val writer = new PrintWriter(new File("./data/CuttingResult.txt"))
    for (str <- resarray) {
      writer.write("【原始】" + str.replace(" ", "") + "\n")
      writer.write("【分词】" + str + "\n")
      writer.write("\n")
    }
    writer.close()
  }

}

