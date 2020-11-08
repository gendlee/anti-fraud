package com.gendlee.graph

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}

object GraphX {
    def run(sc: SparkContext): Unit = {

        val srcGraph = createGraph2(sc, "data\\person.txt", "data\\social.txt")
        renderGraph(srcGraph)

    }

    def renderGraph(srcGraph: Graph[(String, String), String]): Unit = {
        val graphStream: SingleGraph = new SingleGraph("graphStream")
        //    load the graphx vertices into GraphStream
        for ((id, name) <- srcGraph.vertices.collect()) {
            val node = graphStream.addNode(id.toString).asInstanceOf[SingleNode]
            node.addAttribute("ui.label", name._1)
        }

        //    load the graphx edges into GraphStream edges
        for (Edge(x, y, relation) <- srcGraph.edges.collect()) {
            val edge = graphStream.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
            edge.addAttribute("ui.label", relation)
        }

        graphStream.setAttribute("ui.quality")
        graphStream.setAttribute("ui.antialias")

        graphStream.display()
    }

    def printGraph(graph: Graph[(String, Int), Int]): Unit = {
        graph.vertices.filter { case (id, (name, age)) => age > 30 }
                .collect()
                .foreach { case (id, (name, age)) => println(s"$name is $age")
                }
        for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
            println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
        }
    }

    def creatGraph1(sc: SparkContext): Graph[(String, Int), Int] = {
        val graph: Graph[(String, Int), Int] = Graph(creatVertexRDD(sc), creatEdgeRDD(sc))
        graph
    }
    def createGraph2(sc: SparkContext, path1: String, path2: String) = {
        // 顶点RDD[顶点的id,顶点的属性值]
        val users: RDD[(VertexId, (String, String))] = sc.textFile(path1).map { line =>
            val vertexId = line.split(" ")(0).toLong
            val vertexName = line.split(" ")(1)
            (vertexId, (vertexName, vertexName))
        }
        // 边RDD[起始点id,终点id，边的属性（边的标注,边的权重等）]
        val relationships: RDD[Edge[String]] = sc.textFile(path2).map { line =>
            val arr = line.split(" ")
            val edge = Edge(arr(0).toLong, arr(2).toLong, arr(1))
            edge
        }
        val defaultUser = ("John Doe", "Missing")
        Graph(users, relationships, defaultUser)
    }

    def creatVertexRDD(sc: SparkContext): RDD[(Long, (String, Int))] = {
        val vertexArray = Array(
            (1L, ("Alice", 28)),
            (2L, ("Bob", 27)),
            (3L, ("Charlie", 65)),
            (4L, ("David", 42)),
            (5L, ("Ed", 55)),
            (6L, ("Fran", 50))
        )
        sc.parallelize(vertexArray)
    }

    def creatEdgeRDD(sc: SparkContext): RDD[Edge[Int]] = {
        val edgeArray = Array(
            Edge(2L, 1L, 7),
            Edge(2L, 4L, 2),
            Edge(3L, 2L, 4),
            Edge(3L, 6L, 3),
            Edge(4L, 1L, 1),
            Edge(5L, 2L, 2),
            Edge(5L, 3L, 8),
            Edge(5L, 6L, 3)
        )
        sc.parallelize(edgeArray)
    }

}
