#From the command line to view the hdfs command line options
$hdfs dfs
#Copy file foo.txt from local disk to the user's directory in HDFS
$hdfs dfs -put foo.txt foo.txt
#insert a directory into a HDFS from local disk
$hdfs dfs -put kb /loudacre/
#Get a directory listing of the user's home directory in HDFS
$hdfs dfs -ls
#get a directory listing of the HDFS root directory 
$hdfs dfs -ls /
#display the contents of the HDFS file /user/fred/bar.txt
$hdfs dfs -cat /user/fred/bar.txt
#copy that file to the local disk, named as baz.txt
$hdfs dfs -get /user/fred/bar.txt baz.txt
#copy an entire directory from HDFS to local disk
$hdfs dfs -get /user/hive/warehouse/device /tmp/device
#create a directory called input under the user's home directory
$hdfs dfs -mkdir input
#NOTE: copyFromLocal is a synonym for put; copyToLocal is a synonym for get

#delete a file
$hdfs dfs -rm input_old/file1
#delete a set of files using a wildcard
$hdfs dfs -rm input_old/*
#delete the directory input_old and all its contents
$hdfs dfs -rm -r input_old
#viewing HDFS files
$hdfs dfs -cat /loudacre/kb/KBDOC-00289.html | head -n 20
$hdfs dfs -cat /loudacre/kb/KBDOC-00289.html | tail -n 20
$hdfs dfs -cat /loudacre/kb/KBDOC-00289.html | more -n 20
$hdfs dfs -cat /loudacre/kb/KBDOC-00289.html | less -n 5
#the "help" of the command line
$hdfs dfs -help <command>
$hdfs dfs -help cat

#reading a parquet file from command line
$parquet-tools head hdfs://localhost/loudacre/accounts/myfile.parquet
$parquet-tools schema hdfs://localhost/loudacre/accounts/myfile.parquet
#reading a avro file from command line
$avro-tools getschema hdfs://localhost/loudacre/accounts/part-m-00000.avro
$avro-tools cat --offset 1 --limit 5 hdfs://localhost/loudacre/accounts/part-m-00000.avro -