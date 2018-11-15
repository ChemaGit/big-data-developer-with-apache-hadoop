//Answer : See the explanation for Step by Step Solution and configuration.

//Explanation: Solution : 
//A. x.split(",") 
//B. if (x.isEmpty) 0 else x

val field = sc.textFile("spark15/file1.txt")
val mapper = field.map(x => x.split(","))
mapper.map(x => x.map(x=> {if (x.isEmpty || x == " ") 0 else x})).collect
//Other solution
mapper.map(x => x.map(x => {if(x == "" || x == " ") "0" else x})).collect