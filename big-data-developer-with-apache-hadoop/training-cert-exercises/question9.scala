/** Question 9
  * Problem Scenario 89 : You have been given below patient data in csv format without header
  * The local directory is: /home/cloudera/files/patients.csv
  * Put the file in a hdfs directory
  * patientID,name,dateOfBirth,lastVisitDate
  * 1001,Ah Teck,1991-12-31,2012-01-20
  * 1002,Kumar,2011-10-29,2012-09-20
  * 1003,Ali,2011-01-30,2012-10-21
  * Accomplish following activities.
  * 1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
  * 2. Find all the patients who born in 2011
  * 3. Find all the patients age
  * 4. List patients whose last visited more than 60 days ago
  * 5. Select patients 18 years old or younger
  */
$ hdfs dfs -put /home/cloudera/files/patients.csv /user/cloudera/files

val patients = sc.textFile("/user/cloudera/files/patients.csv").map(line => line.split(",")).map(r => (r(0),r(1),r(2),r(3))).toDF("id","name","birth","lastVisit")
patients.show()
patients.registerTempTable("patients")

// 1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
sqlContext.sql("""SELECT id,name,birth,lastVisit FROM patients WHERE to_unix_timestamp(lastVisit,"yyyy-MM-dd") >= to_unix_timestamp("2012-09-15","yyyy-MM-dd")""").show()

// 2. Find all the patients who born in 2011
sqlContext.sql("""SELECT id,name,birth,lastVisit FROM patients WHERE substr(birth,0,4) = "2011" """).show()

// 3. Find all the patients age
sqlContext.sql("""SELECT id,name,birth,lastVisit,cast(round(datediff(current_date, birth)/365,0) as int) as age FROM patients""").show()

// 4. List patients whose last visited more than 60 days ago
sqlContext.sql("""SELECT id,name,birth,lastVisit FROM patients WHERE datediff(current_date,lastVisit) > 60""").show()

// 5. Select patients 18 years old or younger
sqlContext.sql("""SELECT id,name,birth,lastVisit,cast(round(datediff(current_date, birth)/365,0) as int) as age FROM patients WHERE cast(round(datediff(current_date, birth)/365,0) as int) <= 18""").show()