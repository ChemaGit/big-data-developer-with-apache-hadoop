// $ mvn package
// $ spark-submit --class example.GradosSeparacionB --name 'Grados de Separacion' --master yarn-client target/grados-separacion-1.0.jar /files/Marvel-graph.txt
// $ spark-submit --class example.GradosSeparacionB --name 'Grados de Separacion' --master 'local[*]' target/grados-separacion-1.0.jar /files/Marvel-graph.txt
// $ spark-submit --class example.GradosSeparacionB --name 'Grados de Separacion' --master 'local[8]' target/grados-separacion-1.0.jar /files/Marvel-graph.txt

package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Accumulator

/**
 * Ejercio Marvel Graph
 * Todas las lineas tienen numeros
 * El primer numero es la identificacion de un heroe de Marvel
 * El resto de numeros son id's de heroes de marvel que han salido alguna vez en alguna peli­cula con el
 * Hay que calcular para un heroe la distancia que hay con el resto de heroes
 * Implementar un algoritmo: Breadth-first search
 */

/**
 * En el anterior damos un origen y tenemos que calcular la distancia a otro id. En éste
 * tenemos que calcular la distancia de todos los puntos. Es decir, damos un origen y
 * calculamos la distancia de todos los puntos al origen.
 * Hay que hacer iteraciones hasta que no quede ningún vértice de color gris. Al final
 * por cada distancia, hay que imprimir la cantidad de ids que están a esa distancia del origen.
 * Después de cada reduce, hay que usar la función noHayGrises que devuelve True si
 * no queda más grises en el RDD.
 */

/**
 * Lo mismo que el 2a excepto que ahora para detectar el final del ciclo no usamos la
 * función noHayGrises. Tenemos un acumulador, numGrises, que incializamos a
 * cero. Luego cuando la función bfsMap crea un nuevo nodo de color gris incrementa el acumulador.
 * Después de hacer el map/reduce en el main comprobamos el valor del acumulador. Si
 * es mayor que 0, lo ponemos a 0 y seguimos en el bucle. Si es 0 el bucle ha terminado.
 * En el main, después de hacer el reduce, ejecutamos take(1) para provcar una
 * acción. Si no lo hacemos, no se ejecuta el map ni el reduce ya que Spark trabaja en
 * modo lazy. Y entonces el programa se termina.
 *
 *************EJERCICIO RESUELTO CON ACUMULADOR***********************
 * Los acumuladores son variables globales para su ejecucion paralela distribuida.
 * Nosotros modificamos la variable y Spark lleva el manejo de la variable internamente
 * ya que esa variable sera modificada por todos los procesos que corren en otras maquinas.
 */
object GradosSeparacionB {

  val B = "BLANCO"
  val G = "GRIS"
  val N = "NEGRO"

  def setInit(line: (String, (Array[String], Int, String)) ): (String, (Array[String], Int, String)) = {
	if(line._1.equals("1")) {		
		(line._1,(line._2._1,0,G))
	} else line
  }


  def changeToBlack(value: (Array[String], Int, String) ): (Array[String], Int, String) = {
	if(value._3.equals(G))  (value._1,value._2 ,N)
	else value	
  }

  def mixValues(v1: (Array[String], Int, String) ,v2: (Array[String], Int, String) ): (Array[String], Int, String)  = {
	if(v1._3.equals(N)) v1
	else if (v2._3.equals(N)) v2
	else if(v1._1(0).equals("")) {
		val distancia = v1._2
		val color = v1._3
		(v2._1, distancia, color)
	} else {
		val distancia = v2._2
		val color = v2._3
		(v1._1, distancia, color)		
	}
  }

  def buildMap(r: Array[(String, (Array[String], Int, String))]):Map[Int,Int] = {
	val t = r.length
	val mapR: Map[Int,Int] = Map()	

	@annotation.tailrec
	def loadMap(tope: Int, count: Int, arr: Array[(String, (Array[String], Int, String))],myMap:Map[Int,Int]): Map[Int,Int] = {
		val v = arr(count)
		val dis = v._2._2
		val nodos = if(myMap.contains(dis)) myMap.apply(dis) + 1				
		            else 1 
		val myMapUpd = myMap.updated(dis,nodos)		
		if(count == tope - 1) {				
			myMapUpd			
		} else {
			loadMap(t,count + 1,arr,myMapUpd)
		}
	}

	loadMap(t,0,r,mapR)
  }

  def printMap(m: Map[Int,Int]): Unit = {
	m.foreach((kv) => println("Distancia: %d ** Nodos: %d".format(kv._1, kv._2) ))
  }

  def iterar(acumulador: Accumulator[Int], heroesM: org.apache.spark.rdd.RDD[(String, (Array[String], Int, String))]):org.apache.spark.rdd.RDD[(String, (Array[String], Int, String))] = {
	acumulador.setValue(0)

	val grises = heroesM.filter{case (k,(hs,d,c)) => c.equals(G)}

	acumulador.add(grises.count().toInt)
		    
	println("El contador de grises esta a %d".format(acumulador.localValue))

	if(acumulador.localValue > 0) {
		val heroesM1 = heroesM.mapValues(l => changeToBlack(l) )	

		val distancia = grises.take(1)(0)._2._2

		val keys = grises.flatMap{case (k,(hs,d,c)) => hs}

		val newRdd = keys.map(k => (k, (Array(""), distancia + 1, G)))

		val heroesM2 = heroesM1 ++ newRdd

		val heroesM3 = heroesM2.reduceByKey{case ((hs,d,c),(hs1,d1,c1))  => mixValues((hs,d,c),(hs1,d1,c1))}

		heroesM2.unpersist()

		iterar(acumulador, heroesM3.persist())
	} else {
		heroesM
	}

  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: example.GradosSeparacionA <file>") //loudacre/heroes/Marvel-graph.txt
      System.exit(1)
    }

    val sconf = new SparkConf().setAppName("Grados de Separacion").set("spark.ui.port","4141")
    val sc = new SparkContext(sconf)

    val acumulador = sc.accumulator(0)

    sc.setLogLevel("ERROR")

    val myFile = args(0)

    val infinity = 999999
    
    val heroes = iterar(acumulador, sc.textFile(myFile).map(l => (l.split(' ')(0), (l.split(' ').tail, infinity, B))).map(l => setInit(l)))
    //Mapa -> {distancia:numero de nodos}
    printMap(buildMap(heroes.collect))

    val mapHeroes = heroes.map(tuple => tuple._1 + "," + tuple._2._1.mkString(",") + "," + tuple._2._2 + "," + tuple._2._3)
    mapHeroes.repartition(1).saveAsTextFile("/loudacre/heroes/")
    sc.stop()

  }
}
