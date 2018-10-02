/**
 * carpeta compartida tres ficheros.
 * empleado nombre: codigo, nombre
 * empleado sueldo
 * empleado, jefe
 *
 * crear un fichero que contenga las lineas de la siguiente manera
 * E01, pedro, 3500, azucena
 * ordenados por idEmpleado
 * idEmpleado, Empleado, Sueldo, Jefe
 * 
 * Exactamente lo mismo ordenados por el sueldo de mayor a menor
 */
 val dirEmp = "/loudacre/employee/EmpleadoNombre.csv"
 val dirJefe = "/loudacre/employee/EmpleadoJefe.csv"
 val dirSueldo = "/loudacre/employee/EmpleadoSueldo.csv"

 val rddEmp = sc.textFile(dirEmp).map({case(line) => line.split(",")})
 val rddJefe = sc.textFile(dirJefe).map({case(line) => line.split(",")})
 val rddSueldo = sc.textFile(dirSueldo).map({case(line) => line.split(",")})

 val rddE = rddEmp.map({case(arr) => (arr(0), arr(1))})
 val rddJ = rddJefe.map({case(arr) => (arr(0), arr(1))})
 val rddS = rddSueldo.map({case(arr) => (arr(0), arr(1))})

 val rddJoin = rddE.join(rddS).join(rddJ).sortByKey()
 val pattern = rddJoin.map({case( (idEmp,((emp, sal),boss) )) => idEmp + " " + emp + " " + sal + " " + boss})
 pattern.repartition(1).saveAsTextFile("/loudacre/employee/res")

 val rddJoinB = rddE.join(rddS).join(rddJ).map({case( (id,((emp,sal),boss)) ) => (id,emp,sal.toInt,boss) }).sortBy(p => p._3, ascending=false)
 val patternB = rddJoinB.map({case(id,emp,sal,boss) => id + " " + emp + " " + sal + " " + boss})
 patternB.repartition(1).saveAsTextFile("/loudacre/employee/res1")