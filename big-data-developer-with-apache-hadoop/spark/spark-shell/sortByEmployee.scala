/**
 * 	 E01,Lokesh
 *   E02,Bhupesh
 *   E03,Amit
 *   E04,Ratan
 *   E05,Dinesh
 *   E06,Pavan
 *   E07,Tejas
 *   E08,Sheela
 *   E09,Kumar
 *   E10,Venkat
 * 
 * read the file EmployeeName.csv and sort the records by name
 * save the result in a directory called /loudacre/files/employees
 * in a single file
 */

  val myDir = "/loudacre/files/EmployeeName.csv"
  val outDir = "/loudacre/files/employees/"
  val rdd = sc.textFile(myDir)
              .map(line => line.split(","))
              .sortBy(r => r(1), ascending = false, numPartitions = 1)
              .map(line => line.mkString(","))
              
  rdd.saveAsTextFile(outDir)            