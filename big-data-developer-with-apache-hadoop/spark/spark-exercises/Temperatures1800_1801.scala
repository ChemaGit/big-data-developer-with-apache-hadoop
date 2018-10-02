/**
  * En esta versión hay que sacar la temperatura media por estación y por año. La
  * diferencia con lo anterior es que ahora puede haber más años aparte del 1800. Por ejemplo.
  * 1800_1801.csv
  * ITE00100554,18000101,TMAX,-75,,,E,
  * ITE00100554,18000101,TMIN,-148,,,E,
  * GM000010962,18000101,PRCP,0,,,E,
  * EZE00100082,18000101,TMAX,-86,,,E,
  * ...
  * EZE00100082,18010115,TMIN,-23,,,E,
  * ITE00100554,18010116,TMAX,56,,,E,
  * ITE00100554,18010116,TMIN,41,,,E,
  * GM000010962,18010116,PRCP,0,,,E,
  * salida
  * 1800 EZE00100082 7.70F
  * 1800 ITE00100554 5.36F
  * 1801 EZE00100082 5.90F
  * 1801 ITE00100554 2.30F
  */
val dirTemp = "/loudacre/temperatures/1800_1801.csv"
val rddTemp = sc.textFile(dirTemp).map({case(line) => line.split(",")}).map({case(arr) => ( (arr(0), arr(1).substring(0,4)), arr(2), arr(3).toDouble)})
val rddFilt = rddTemp.filter({case(k,p,t) => p == "TMIN" || p == "TMAX"}).map({case(k,p,t) => (k,t)})
val groupRdd = rddFilt.groupByKey()
val sumRdd = groupRdd.mapValues({case(v) => ((v.sum / v.size) * 0.1) * (9.0 / 5.0) + 32})
val pattern = sumRdd.map({case(((e,y),t)) => y + "\t" + e + "\t" + "%.2fF".format(t)})