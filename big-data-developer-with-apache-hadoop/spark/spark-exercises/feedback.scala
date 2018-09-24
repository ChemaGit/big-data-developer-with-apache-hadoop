/**
 * Cris|11 Jan, 2015|5
 * Pedro|Jan 11, 2015|5
 * Lucas||5
 * Armando|28-02-2013|5
 * Vega|28 Sep, 1998|5
 * 
 * find out and write bad dates and good dates in separated files
 */
def dateOk( date: String ) = {
    val reg1 = """\d{1,2}\s\w{3},\s\d{4}""".r
    reg1.findFirstIn( date ).nonEmpty
}

val dir = "/loudacre/files/feedback.txt"

val feedback = sc.textFile( dir ).map( _.split( '|' ) )
    
val good = feedback.filter( el =>  dateOk( el(1) ) ).map( _.mkString("|") )
good.saveAsTextFile( "/loudacre/files/good")

val bad = feedback.filter( el =>  ! dateOk( el(1) ) ).map( _.mkString("|") )
bad.saveAsTextFile( "/loudacre/files/bad")