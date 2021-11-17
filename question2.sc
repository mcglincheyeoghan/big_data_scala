//import GraphX packages
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
//import classes required for using GraphX
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

/***************************************************************************************/
//question1
//define the co-authorship schema
case class Coauthorship(author1:String, author2:String)

//function to parse input into Coauthorship class
def parseCoauthorship(str: String): Coauthorship = {
	val line = str.split(",")
	Coauthorship(line(0), line(1))
}

//load the data into an RDD variable
var textRDD = sc.textFile("dblp_coauthorship.csv")

//assign the header of the CSV file to the header variable since it cannot be modified
val header = textRDD.first()

//filter out the header since it  cannot be parsed to a Coauthorship
textRDD = textRDD.filter(row => row != header)

//parse the RDD of CSV lines into an RDD of Coauthorship classes
val coauthorshipRDD = textRDD.map(parseCoauthorship).cache()

//create author ids with ID and name
val authorWithId = coauthorshipRDD.flatMap(paper => Seq((paper.author1))).distinct.zipWithIndex
//swap the authorWithId variable so that it's (authorId, authorName)
val authorWithIdSwap = authorWithId.map(_.swap)

//define a default vertex called nowhere
val nowhere = "nowhere"

//map author id to the author
val authorMap = authorWithId.map {
	case (name, id) => (name, id)
}.collect.toMap

//create RDD with author1Id and author2Id
val routes = coauthorshipRDD.map(paper => (authorMap.get(paper.author1), authorMap.get(paper.author2))).distinct

//define a distance variable and let it equal 1
val distance = 1

//create edges RDD with author1Id, author2Id and distance
val edges = routes.map {
	case (id1, id2) => Edge(id1.get, id2.get, distance)
}

//define the graph
val graph = Graph(authorWithIdSwap, edges, nowhere)

/***************************************************************************************/
//question2

//import required package to compute shortest paths
import org.apache.spark.graphx.lib.ShortestPaths
//get the Id for Paul Erdös
val erdos = authorMap.get("Paul Erdös").get
//find the distances of the shortest paths between Erdös and the rest of the authors
val shortestDistances = ShortestPaths.run(graph,Seq(erdos)).vertices.collect
//get the distances from the tuples
val distancesOnly = shortestDistances.map(value => value._2).map(value => value.values)
//diamter is the longest shortest path between any the Paul Erdös node and any other node in the graph
val diameter = distancesOnly.map(path => path.max).max

print("The maximum Erdös number of the authors in this graph: ")
println(diameter)

System.exit(0)