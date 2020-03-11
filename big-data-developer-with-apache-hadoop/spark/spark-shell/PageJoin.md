````scala
// Example: Join a list of Knowledge Base articles with article requests from a web server log 

// parse a line from a web log file to find the ID of the knowledge base article requested
// return None if not a KB request
val matchre = "KBDOC-[0-9]*".r
def getRequestDoc(s: String): String = { matchre.findFirstIn(s).orNull }

// read in file of knowledge base articles 
// line format is docid:title
var pagefile = "file:/home/training/training_materials/devsh/examples/example-data/kblist.txt"
var pages =  sc.textFile(pagefile).
   map(line => line.split(":")).
   map(tokens => (tokens(0),tokens(1)))

// key logs by page ID requested
var logfile = "file:/home/training/training_materials/data/weblogs/*"      
var logs = sc.textFile(logfile).
    keyBy(getRequestDoc(_)).
    filter(_._1 != null)

// join the list of articles with the list of requests and display the first 10
var pagelogs = logs.join(pages)

pagelogs.take(10).foreach(println)
````

