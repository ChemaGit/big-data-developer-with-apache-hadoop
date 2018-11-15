/** Question 66
 * Problem Scenario 2 :
 * There is a parent organization called "ABC Group Inc", which has two child companies named Tech Inc and MPTech.
 * Both companies employee information is given in two separate text file as below. Please do the following activity for employee details.
 * TechInc.txt
 * 1,Alok,Hyderabad
 * 2,Krish,Hongkong
 * 3,Jyoti,Mumbai
 * 4,Atul,Banglore
 * 5,Ishan,Gurgaon
 * MPTech.txt
 * 6,John,Newyork
 * 7,alp2004,California
 * 8,tellme,Mumbai
 * 9,Gagan21,Pune
 * 10,Mukesh,Chennai
 * 1. Which command will you use to check all the available command line options on HDFS and How will you get the Help for individual command.
 * 2. Create a new Empty Directory named Employee using Command line. And also create an empty file named in it Techinc.txt
 * 3. Load both companies Employee data in Employee directory (How to override existing file in HDFS).
 * 4. Merge both the Employees data in a Single tile called MergedEmployee.txt, merged tiles should have new line character at the end of each file content.
 * 5. Upload merged file on HDFS and change the file permission on HDFS merged file, so that owner and group member can read and write, other user can read the file.
 * 6. Write a command to export the individual file as well as entire directory from HDFS to local file System.
 */

//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 

//Step 1 : Check All Available command 
$ hdfs dfs 

//Step 2 : Get help on Individual command 
$ hdfs dfs -help <command>
$ hdfs dfs -help get 

//Step 3 : Create a directory in HDFS using named Employee and create a Dummy file in it called e.g. Techinc.txt 
$ hdfs dfs -mkdir /files/Employee 
//Now create an empty file in Employee directory using Hue. 

//Step 4 : Create a directory on Local file System and then Create two files, with the given data in problems. 
$ mkdir Employee
$ cd Employee
$ gedit Techinc.txt MPTech.txt & //Edit the files

//Step 5 : Now we have an existing directory with content in it, now using HDFS command line , overrid this existing Employee directory. 
//While copying these files from local file System to HDFS.  
$ hdfs dfs -put -f Techinc.txt MPTech.txt /files/Employee 

//Step 6 : Check All files in directory copied successfully 
hdfs dfs -ls /files/Employee 

//Step 7 : Now merge all the files in Employee directory, 
$ hdfs dfs -getmerge -nl /files/Employee Employee/MergedEmployee.txt 

//Step 8 : Check the content of the file. 
$ cat Employee/MergedEmployee.txt 

//Step 9 : Copy merged file in Employeed directory from local file ssytem to HDFS. 
$ hdfs dfs -put Employee/MergedEmployee.txt /files/Employee/ 

//Step 10 : Check file copied or not. 
$ hdfs dfs -ls /files/Employee 

//Step 11 : Change the permission of the merged file on HDFS 
$ hdfs dfs -chmod 664 /files/Employee/MergedEmployee.txt 

//Step 12 : Get the file from HDFS to local file system, 
$ hdfs dfs -get /files/Employee Employee_hdfs

/**OTHER SOLUTION**/
//Step 1: Which command will you use to check all the available command line options on HDFS and How will you get the Help for individual command.
$ hdfs dfs
$ hdfs dfs -help
$ hdfs dfs -help <command>

//Step 2: Create a new Empty Directory named Employee using Command line. And also create an empty file named in it Techinc.txt
$ hdfs dfs -mkdir /files/Employee
$ gedit Techinc.txt &
$ hdfs dfs -put Techinc.txt /files/Employee

//Step 3: Load both companies Employee data in Employee directory (How to override existing file in HDFS).
$ hdfs dfs -put -f Techinc.txt MPTech.txt /files/Employee

//Step 4: Merge both the Employees data in a Single file called MergedEmployee.txt, merged files should have new line character at the end of each file content.
$ hdfs dfs -appendToFile Techinc.txt MPTech.txt /files/Employee/MergedEmployee.txt 

//Step 5: Upload merged file on HDFS and change the file permission on HDFS merged file, so that owner and group member can read and write, other user can read the file.
$ hdfs dfs -chmod 477 /files/Employee/MergedEmployee.txt

//Step 6: Write a command to export the individual file as well as entire directory from HDFS to local file System.
$ hdfs dfs -get /files/Employee /home/training/Desktop/files