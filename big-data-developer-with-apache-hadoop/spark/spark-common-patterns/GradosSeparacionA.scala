// $ mvn package
// $ spark-submit --class example.GradosSeparacionA --name 'Grados de Separacion' --master yarn-client target/grados-separacion-1.0.jar /files/Marvel-graph.txt
// $ spark-submit --class example.GradosSeparacionA --name 'Grados de Separacion' --master 'local[*]' target/grados-separacion-1.0.jar /files/Marvel-graph.txt
// $ spark-submit --class example.GradosSeparacionA --name 'Grados de Separacion' --master 'local[8]' target/grados-separacion-1.0.jar /files/Marvel-graph.txt

package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Ejercio Marvel Graph
 * Todas las lineas tienen numeros
 * El primer numero es la identificacion de un heroe de Marvel
 * El resto de numeros son id's de heroes de marvel que han salido alguna vez en alguna peli�cula con el
 * Hay que calcular para un heroe la distancia que hay con el resto de heroes
 * Implementar un algoritmo: Breadth-first search
 */

/**
 * En el anterior damos un origen y tenemos que calcular la distancia a otro id. En �ste
 * tenemos que calcular la distancia de todos los puntos. Es decir, damos un origen y
 * calculamos la distancia de todos los puntos al origen.
 * Hay que hacer iteraciones hasta que no quede ning�n v�rtice de color gris. Al final
 * por cada distancia, hay que imprimir la cantidad de ids que est�n a esa distancia del origen.
 * Despu�s de cada reduce, hay que usar la funci�n noHayGrises que devuelve True si
 * no queda m�s grises en el RDD.
 */
object GradosSeparacionA {
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

  /*
   * Recorre un Array para comprobar si cumple una condicion
   * Utiliza tailrecursion para recorrer el array
   */
  def noHayGrises(r: Array[(String, (Array[String], Int, String))]): Boolean = {
	val tope = r.length
	@annotation.tailrec
	def check(count: Int): Boolean = {
		val v = r(count)
		val grises = v._2._3.equals(G)
		if(count == tope - 1) grises						
		else 
			if(grises) grises
			else check(count + 1)
	}	
	
	check(0)
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

  def iterar(heroesM: org.apache.spark.rdd.RDD[(String, (Array[String], Int, String))]):org.apache.spark.rdd.RDD[(String, (Array[String], Int, String))] = {
	val grises = heroesM.filter{case (k,(hs,d,c)) => c.equals(G)}

	val heroesM1 = heroesM.mapValues(l => changeToBlack(l) )

	val distancia = grises.take(1)(0)._2._2

	val keys = grises.flatMap{case (k,(hs,d,c)) => hs}

	val newRdd = keys.map(k => (k, (Array(""), distancia + 1, G)))

	val heroesM2 = heroesM1 ++ newRdd

	val heroesM3 = heroesM2.reduceByKey{case ((hs,d,c),(hs1,d1,c1))  => mixValues((hs,d,c),(hs1,d1,c1))}

	heroesM2.unpersist()

	val continua = noHayGrises(heroesM3.collect)
	
	if(!continua) heroesM3
	else iterar(heroesM3.persist())
  }


  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: example.GradosSeparacionA <file>") //loudacre/heroes/Marvel-graph.txt
      System.exit(1)
    }

    val sconf = new SparkConf().setAppName("Grados de Separacion").set("spark.ui.port","4141")
    val sc = new SparkContext(sconf)
    sc.setLogLevel("ERROR")

    val myFile = args(0)

    val infinity = 999999
    
    val heroes = iterar(sc.textFile(myFile).map(l => (l.split(' ')(0), (l.split(' ').tail, infinity, B))).map(l => setInit(l)))
    //Mapa -> {distancia:numero de nodos}
    printMap(buildMap(heroes.collect))

    val mapHeroes = heroes.map(tuple => tuple._1 + "," + tuple._2._1.mkString(",") + "," + tuple._2._2 + "," + tuple._2._3)
    mapHeroes.saveAsTextFile("/loudacre/heroes/")
    sc.stop()
  }
}


/*************************FUNCIONA***************************************/
/*
var heroes = sc.textFile(myFile).map(l => (l.split(' ')(0), (l.split(' ').tail, infinity, B))).map(l => setInit(l))
var continua = true

while(continua) {
	var grises = heroes.filter{case (k,(hs,d,c)) => c.equals(G)}

	heroes = heroes.mapValues(l => changeToBlack(l) )

	var distancia = grises.take(1)(0)._2._2

	var keys = grises.flatMap{case (k,(hs,d,c)) => hs}

	var newRdd = keys.map(k => (k, (Array(""), distancia + 1, G)))

	heroes = heroes ++ newRdd

	heroes = heroes.reduceByKey{case ((hs,d,c),(hs1,d1,c1))  => mixValues((hs,d,c),(hs1,d1,c1))}

	continua = noHayGrises(heroes.collect)
}
*/
