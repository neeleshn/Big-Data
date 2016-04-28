
import scala.annotation.tailrec
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object ShortestPath {
    // Node = (name, neibs, prev, dist)
    type Node = Tuple4[String, Array[String], String, Double]
    // Link = (prev, dist)
    type Link = Tuple2[String, Double]

    var src = ""
    var dst = ""

    def main(args: Array[String]) {
        if (args.length < 3) {
            println("Usage:")
            println("   shortest_path Graph From To")
            return
        }

        val input = args(0)
        src = args(1)
        dst = args(2)

        val conf = new SparkConf().
            setAppName("Shortest Path").
            setMaster("local")
        val sc = new SparkContext(conf)
        
        val lines = sc.textFile(input)
    
        // Create initial graph.
        val nodes0 = lines.map(makeNode)

        // Iterate until destination has
        // a non-infinite distance set.
        val nodes1 = iterate(nodes0, sc)

        // Follow back-pointers from dst to src.
        val path = findPath(nodes1, dst, sc)
        
        sc.stop()

        // Print out the path.
        path.foreach(n => println(n))
    }

    def findPath(nodes : RDD[Node], name : String, sc : SparkContext) : Array[String] = {
        if (name == src) {
            return Array(name)
        }

        // Find the node named "name", extract previous
        // reference.
        val (_, _, prev, _) = nodes.filter(_._1 == name).first

        // Find path to previous, append current.
        return findPath(nodes, prev, sc) :+ name
    }

    @tailrec
    def iterate(nodes : RDD[Node], sc : SparkContext) : RDD[Node] = {
        // TODO: Implement me.

        // Task: 
        //  - Perform one step of the algorithm.
        //  - Input: Set of nodes.
        //  - Output: Set of nodes.
        //  - Some nodes in the inputs have "dist" set.
        //  - Any node in the input with +inf dist and
        //    a neighbor with a known distance should
        //    have a known distance in the output.
    }

    def makeNode(line : String) : Node = {
        val words = line.split("\\s+")

        val name  = words(0)
        val neibs = words.slice(1, words.length)
        val dist  = if (words(0) == src) 0 else Double.PositiveInfinity

        return new Node(name, neibs, "", dist)
    }

    def nextLinks(node : Node) : Array[Tuple2[String, Link]] = {
        val (name, neibs, _, dist) = node

        // No work to do on neighbors if we don't
        // have a distance to this node yet.
        if (dist == Double.PositiveInfinity) {
            return Array()
        }
        
        // For nodes with distances, we know of
        // a path to each neighbor of distance + 1.
        return neibs.map(neib =>
            (neib, (name, dist + 1))
        )
    }

    def nextNode(step : Tuple2[String, Tuple2[Iterable[Node], Iterable[Link]]]) : Node = {
        val (name, (nodes, links)) = step

        // We've collected the *incoming* edges to each
        // node. Find the one with minimum cost.
        val (from, best) = if (links.size == 0) ("", Double.PositiveInfinity) else links.minBy(_._2)

        if (nodes.size == 0) {
            return (name, Array(), from, best)
        }

        val (_, neibs, prev, dist)  = nodes.head

        // If the cheapest incoming edge is better than
        // the current cost, that's our new node.
        if (best < dist) {
            return (name, neibs, from, best)
        }
        else {
            return nodes.head
        }
    }
}

// vim: set ts=4 sw=4 et:
