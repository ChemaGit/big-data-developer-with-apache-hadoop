//write	code to go through	a set of activation XML files and	
//extract the account number and device	model for each activation, and save the	list to	
//a file as
import scala.xml._

// Given a string containing XML, parse the string, and 
// return an iterator of activation XML records (Nodes) contained in the string
def getActivations(xmlstring: String): Iterator[Node] = {
    val nodes = XML.loadString(xmlstring) \\ "activation"
    nodes.toIterator
}

// Given an activation record (XML Node), return the model name
def getModel(activation: Node): String = {
   (activation \ "model").text
}

// Given an activation record (XML Node), return the account number
def getAccount(activation: Node): String = {
   (activation \ "account-number").text
}
val dir = "/loudacre/activations"
val records = sc.wholeTextFiles(dir)
val nodes = records.flatMap(file => getActivations(file._2))
val result = nodes.map(record => getAccount(record) + ":" + getModel(getModel))
result.saveAsTextFile("/loudacre/account-models")