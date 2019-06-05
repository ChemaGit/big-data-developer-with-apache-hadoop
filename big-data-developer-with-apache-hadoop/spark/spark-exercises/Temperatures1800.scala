/**
 * Tenemos mediciones de temperatura del año 1800. Hay que averiguar por cada
 * estación la temperatura mínima del año. La temperatura actual son los grados Celsius
 * multiplicados por 10. En el resultado, hay que expresar la temperatura en Fahrenheit.
 * Es decir si t es la temperatura que hay en el fichero hay convertirla a
 * t*0,1*(9.0/5.0) + 32.
 * 1800.csv
 * ITE00100554,18000101,TMAX,-75,,,E,
 * ITE00100554,18000101,TMIN,-148,,,E,
 * GM000010962,18000101,PRCP,0,,,E,
 * EZE00100082,18000101,TMAX,-86,,,E,
 * EZE00100082,18000101,TMIN,-135,,,E,
 * ITE00100554,18000102,TMAX,-60,,I,E,
 * En donde:
 * ITE00100554: Código de estación
 * 18000102: Año mes día
 * TMIN: indica que la cifra que sigue es temperatura mínima.
 * -60: Temperatura mínima en Celsius multiplicada por 10.
 * El segundo campo siempre empieza por 1800.
 * salida
 * EZE00100082	 7.70F
 * ITE00100554	 5.36F
 */
 val dirTemp = "/loudacre/temperatures/1800.csv"
 val rddTemp = sc.textFile(dirTemp).map(line => line.split(",")).map(arr => (arr(0), arr(2), arr(3))).filter(arr => arr._2 == "TMIN")
 val rddGroup = rddTemp.map({case(est, min, temp) => (est, temp.toFloat)}).reduceByKey({case(v, v1) => if(v < v1)v else v1})
 val rddFar = rddGroup.mapValues(v => (v * 0.1) * (9.0 / 5.0) + 32)	
 val rddFormat = rddFar.map({case(e, v) => e + " " + "%.2fF".format(v)})
 rddFormat.repartition(1).saveAsTextFile("/loudacre/temperatures/resMin")
 
 /**
 * max-temperature
 * Lo mismo que min-temperatury pero calcular la temperatura máxima.
 */
 val dirTemp = "/files/1800.csv"
 val rddTemp = sc.textFile(dirTemp).map(line => line.split(",")).map(arr => (arr(0),arr(2),arr(3))).filter(arr => arr._2 == "TMAX")
 val rddGroup = rddTemp.map({case(est, pat,temp) => (est, temp.toDouble)}).reduceByKey({case(v, v1) => if(v > v1) v else v1})
 val rddFar = rddGroup.mapValues(v => (v * 0.1) * (9.0 /5.0) + 32) 	
 val rddFormat = rddFar.map({case(e, v) => e + " " + "%.2fF".format(v)})
 rddFormat.repartition(1).saveAsTextFile("/loudacre/temperatures/resMax")
 
 /**
 * max-temperatures_groupByKey
 * Lo mismo que max-temperature pero hay que usar groupByKey.
 */
 val dirTemp = "/files/1800.csv"
 val rddTemp = sc.textFile(dirTemp).map(line => line.split(",")).map(arr => (arr(0), arr(2), arr(3))).filter(arr => arr._2 == "TMAX")
 val rddGroup = rddTemp.map({case(est, pat, temp) => (est, temp.toDouble)}).groupByKey()
 val rddFar = rddGroup.mapValues({case(it) => (it.max * 0.1) * (9.0 / 5.0) + 32})
 val rddFormat = rddFar.map({case(e, t) => e + " " + "%.2fF".format(t)})
 rddFormat.repartition(1).saveAsTextFile("/loudacre/temperatures/resMax_b")