# Question 66
````text
   Problem Scenario 2 :
   There is a parent organization called "ABC Group Inc", which has two child companies named Tech Inc and MPTech.
   Both companies employee information is given in two separate text file as below. Please do the following activity for employee details.
   TechInc.txt
   1,Alok,Hyderabad
   2,Krish,Hongkong
   3,Jyoti,Mumbai
   4,Atul,Banglore
   5,Ishan,Gurgaon
   MPTech.txt
   6,John,Newyork
   7,alp2004,California
   8,tellme,Mumbai
   9,Gagan21,Pune
   10,Mukesh,Chennai
   1. Which command will you use to check all the available command line options on HDFS and How will you get the Help for individual command.
   2. Create a new Empty Directory named Employee using Command line. And also create an empty file named in it Techinc.txt
   3. Load both companies Employee data in Employee directory (How to override existing file in HDFS).
   4. Merge both the Employees data in a Single file called MergedEmployee.txt, merged files should have new line character at the end of each file content.
   5. Upload merged file on HDFS and change the file permission on HDFS merged file, so that owner and group member can read and write, other user can read the file.
   6. Write a command to export the individual file as well as entire directory from HDFS to local file System.
````

````properties  
# create the files into local

$ gedit /home/cloudera/files/TechInc.txt &
$ gedit /home/cloudera/files/MPTech.txt &
# 1
$ hdfs
$ hdfs dfs
$ hdfs dfs -help <command>
$ hdfs dfs -help getmerge
# 2
$ hdfs dfs -mkdir /user/cloudera/question66
$ hdfs dfs -mkdir /user/cloudera/question66/Employee
$ hdfs dfs -touchz /user/cloudera/question66/Employee/Techinc.txt
# 3
$ hdfs dfs -put -f /home/cloudera/files/Techinc.txt /user/cloudera/question66/Employee/
$ hdfs dfs -put -f /home/cloudera/files/MPTech.txt /user/cloudera/question66/Employee/
$ hdfs dfs -ls /user/cloudera/question66/Employee/
$ hdfs dfs -cat /user/cloudera/question66/Employee/Techinc.txt
# 4
$ mkdir /home/cloudera/files/Employee
$ hdfs dfs -getmerge -nl /user/cloudera/question66/Employee/ /home/cloudera/files/Employee/MergedEmployee.txt
$ ls /home/cloudera/files/Employee
$ cat ls /home/cloudera/files/Employee/MergedEmployee.txt
# 5
$ hdfs dfs -put /home/cloudera/files/Employee/MergedEmployee.txt /user/cloudera/question66/Employee/
$ hdfs dfs -chmod 664 /user/cloudera/question66/Employee/MergedEmployee.txt
$ hdfs dfs -ls hdfs dfs -chmod 664 /user/cloudera/question66/Employee/
# 6
$ hdfs dfs -get -p /user/cloudera/question66/Employee /home/cloudera/files/Employee_hdfs
$ ls -ltr /home/cloudera/files/Employee_hdfs
````