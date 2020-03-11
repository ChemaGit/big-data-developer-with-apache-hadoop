````scala
// Example: Use a variable to pass a lookup table of Knowledge Base articles

import scala.io.Source

// parse a line from a web log file to find the ID of the knowledge base article requested
// return None if not a KB request
val matchre = "KBDOC-[0-9]*".r
def getRequestDoc(s: String): String = { matchre.findFirstIn(s).orNull }

// read in file of knowledge base articles 
// line format is docid:title
var pagefile = "/home/training/training_materials/devsh/examples/example-data/kblist.txt"

// read in file of knowledge base articles (pages)
// line format is docid:title
var pages = Source.fromFile(pagefile).
    getLines.map(_.split(":")).map(strings => (strings(0),strings(1))).toMap

// key logs by page ID requested
var logfile = "file:/home/training/training_materials/data/weblogs/*"      
var logs = sc.textFile(logfile).
    keyBy(getRequestDoc(_)).
    filter(_._1 != null)

// join the list of articles with the list of requests and display the first 10
var pagelogs = logs.map(pair => (pages(pair._1),pair._2))

pagelogs.take(10).foreach(println)
````

