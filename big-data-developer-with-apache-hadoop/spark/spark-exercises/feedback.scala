/**
 * Cris|11 Jan, 2015|5
 * Pedro|Jan 11, 2015|5
 * Lucas||5
 * Armando|28-02-2013|5
 * Vega|28 Sep, 1998|5
 * 
 * find out and write bad dates and good dates in separated files
 */
//Step 1: we create the file feedback.txt
$ gedit /home/cloudera/files/feedback.txt
$ hdfs dfs -put /home/cloudera/files/feedback.txt spark9/
//Step 2: 
def dateOk1( date: String ) = {
    val reg1 = """\d{1,2}\s\w{3},\s\d{4}""".r    
    reg1.findFirstIn( date ).nonEmpty
}

def dateOk2( date: String ) = {
    val reg2 = """\d{1,2}\-\d{1,2}\-\d{4}""".r
    reg2.findFirstIn( date ).nonEmpty
}

def dateOk3( date: String ) = {
    val reg3 = """\d{1,2}\/\d{1,2}\/\d{4}""".r
    reg3.findFirstIn( date ).nonEmpty
}

val dir = "/files/feedback.txt"

val feedback = sc.textFile( dir ).map( _.split( '|' ) )
    
val good1 = feedback.filter( el =>  dateOk1( el(1) ) ).map( _.mkString("|") )
val good2 = feedback.filter( el =>  dateOk2( el(1) ) ).map( _.mkString("|") )
val good3 = feedback.filter( el =>  dateOk3( el(1) ) ).map( _.mkString("|") )
val good = good1.union(good2).union(good3)
good.saveAsTextFile( "/files/good")

val bad1 = feedback.filter( el =>  ! dateOk1( el(1) ) ).map( _.mkString("|") )
val bad2 = bad1.map(_.split('|')).filter( el =>  ! dateOk2( el(1) ) ).map( _.mkString("|") )
val bad3 = bad2.map(_.split('|')).filter( el =>  ! dateOk3( el(1) ) ).map( _.mkString("|") )
//val bad = bad1.union(bad2).union(bad3)
bad3.saveAsTextFile( "/files/bad")

/****Other solution*****/
//Answer : See the explanation for Step by Step Solution and configuration. Explanation: Solution : 
//Step 1 : Create a file first using Hue in hdfs. 
//Step 2 : Write all valid regular expressions sysntax for checking whether records are having valid dates or not. 
//11 Jan, 2015 
val reg1 = """(\d+)\s(\w{3})(,)\s(\d{4})""".r
//6/17/2014 
val reg2 = 
//22-08-2013
val reg3 = """(\d+)(-)(\d+)(-)(\d{4})""".r
//Jan 11, 2015 
val reg4 = """(\w{3})\s(\d+)(,)\s(\d{4})""".r
//Step 3 : Load the file as an RDD. 
val feedbackRDD = sc.textFile("spark9/feedback.txt") 
//Step 4 : As data are pipe separated , hence split the same. 
val feedbackSplit = feedbackRDD.map(line => line.split('|')) 
//Step 5 : Now get the valid records as well as , bad records. 
val validRecords = feedbackSplit.filter(x => (reg1.pattern.matcher(x(1).trim).matches|reg2.pattern.matcher(x(1).trim).matches|reg3.pattern.matcher(x(1).trim).matches | reg4.pattern.matcher(x(1).trim).matches)) 
val badRecords = feedbackSplit.filter(x => !(reg1.pattern.matcher(x(1).trim).matches|reg2.pattern.matcher(x(1).trim).matches|reg3.pattern.matcher(x(1).trim).matches | reg4.pattern.matcher(x(1).trim).matches)) 
//Step 6 : Now convert each Array to Strings 
val valid = validRecords.map(e => e.mkString("|")) 
val bad =badRecords.map(e => e.mkString("|")) 
//Step 7 : Save the output as a Text file and output must be written in a single tile, 
valid.saveAsTextFile("spark9/good")
bad.saveAsTextFile("spark9/bad")