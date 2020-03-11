// $ mvn package
// $ spark-submit --class example.SimilitudPeliculas --name 'Similitud Peliculas' --master yarn-client target/similitud-peliculas-1.0.jar 1
// $ spark-submit --class example.SimilitudPeliculas --name 'Similitud Peliculas' --master 'local[*]' target/similitud-peliculas-1.0.jar 2
// $ spark-submit --class example.SimilitudPeliculas --name 'Similitud Peliculas' --master 'local[8]' target/similitud-peliculas-1.0.jar 5


/**
 * similitudes-peliculas.scala
 * Tenemos un fichero con opiniones de usuarios acerca de pel�culas. El formato es:
 * input
 * 196 242 3 881250949
 * 186 302 3 891717742
 * 22 377 1 878887116
 * 244 51 2 880606923
 * �
 * Los campos son: 1) id de usuario, 2) id de pel�cula, 3) puntuaci�n, 4) timestamp.
 * Por cada par de pel�culas, queremos establecer una relaci�n entre ellas para indicar si
 * son similares o no. Con esto, cuando nos muestren una pel�cula podemos mostrar
 * aquellas que son similares.
 * En esta primera fase, vamos a partir de una pel�cula y ver la distancia que hay a otra.
 * El procedimiento para obtener la similitud es
 * 1. Pasar de
 * a. id usuario, id pel�cula, puntuaci�n, timestamp a
 * b. id usuario => (id pel�cula, puntuaci�n)
 * 2. Producto cartesiano. Por cada par de pel�culas de un usuario tenemos ahora:
 * (id usuario, ((id pel�cula1, puntuaci�n1), (id pel�cula2, puntuaci�n2)))
 * 3. Filtramos los duplicados. Cada pel�cula est� asociada a s� misma y por cada par
 * de pel�culas hay dos entradas. Filtramos y elegimos las entradas en las que id
 * pel�cula1 sea menor que id pel�cula2.
 * 4. Hacemos un mapeo para que las claves sean los pares de pel�culas; pasamos de
 * id usuario => ((id pel�cula1, puntuaci�n1), (id pel�cula2, puntuaci�n2)) a
 * (id pelicula1, id pelicula2) => (puntuaci�n1, puntuaci�n2)
 * Es decir, nos olvidamos ya del usuario.
 * 5. Agrupamos los pares de pel�culas. Es decir el par de pel�culas es la clave y el
 * valor todos los pares de opiniones sobres las pel�culas:
 * (pelicula1, pelicula2) => (opinion1, opinion2), (opinion1, opinion2)
 * 6. Ahora ejecutamos la similitud coseno para averiguar los valores. El resultado es
 * un par (valor, n�mero de veces) en el que valor es realmente el resultado y
 * n�mero de veces son las veces que ese par ha sido calificado por un usuario.
 * Por ejemplo, si n�mero de veces es 1 indica que s�lo un usuario ha calificado
 * ese par de pel�culas con lo que no se deber�a tomar en cuenta la similitud. 
 * 7. Al final mostramos las pel�culas similares ordenadas por precisi�n.
 */
package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.math._

object SimilitudPeliculas {
	
  def cargarNombresPeliculas(sc: SparkContext): RDD[(String,(String,String))] = {
    val movies = sc.textFile("/files/peliculas.txt").map(line => line.split('|')).map(arr => (arr(0),(arr(0), arr(1))))
    movies
  }

  def filtrarDuplicados(cartesian: (Int,((Int,Float),(Int,Float)) )): Boolean = {
    cartesian match {
      case( (id,((p1,o1),(p2,o2))) ) => p1 < p2
    }
  }

  def construirPares(cartesian: (Int,((Int,Float),(Int,Float)) )): ((Int, Int),(Float, Float)) = {
    cartesian match {
      case( (id,((p1,o1),(p2,o2))) ) => ((p1, p2),(o1,o2))
    }
  }

  def similitudCoseno(it: Iterable[(Float, Float)]): (Float, Int) = {
    var numeroPares = 0
    var suma_xx, suma_yy, suma_xy = 0.0

    for ((opinionX, opinionY) <- it) {
      suma_xx = suma_xx + (opinionX * opinionX)
      suma_yy = suma_yy + (opinionY * opinionY)
      suma_xy = suma_xy + (opinionX * opinionY)
      numeroPares = numeroPares + 1
    }

    val numerador = suma_xy
    val denominador = sqrt(suma_xx) * sqrt(suma_yy)

    val resultado = if(denominador != 0) (numerador / denominador.toFloat)
                    else denominador

    (resultado.toFloat, numeroPares)
  }

  def cumpleCondiciones(res: ((Int, Int),(Float, Int)), idPelicula: Int): Boolean = {
    val limiteResultado = 0.5
    //val limiteResultado = 0.97
    val limiteVeces = 20
    //val limiteVeces = 50   
    res match {
      case((p1, p2),(r, v)) => {
        val cond1 = (p1 == idPelicula) || (p2 == idPelicula) 
        val cond2 = (r > limiteResultado) && (v > limiteVeces)
        cond1 && cond2
      }
    } 		
  }

  val sconf = new SparkConf().setAppName("Template").set("spark.ui.port","4141")
  val sc = new SparkContext(sconf)

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: example.Template <id>")
      System.err.println("You have to supply an id.")
      System.exit(1)
    }

    sc.setLogLevel("ERROR")

    try {
      val idPelicula = args(0).toInt
      println("Load movies..........")
      val diccionarioNombres = cargarNombresPeliculas(sc).cache
      //diccionarioNombres.collect().foreach(x => println(x))
      val datos = sc.textFile("/files/opiniones.txt")

      // Mapear opiniones a pares key/value: (idUsuario, (idPelicula, opinion))
      val opiniones = datos.map(l => l.split("""\t""")).map(l => (l(0).toInt, (l(1).toInt,l(2).toFloat)))

      //Producto cartesiano
      //(idUsuario, ((idPelicula1, opinion1), (idPelicula2, opinion2)))
      val productoOpiniones = opiniones.join(opiniones)

      //Filtrar duplicados
      val opinionesUnicas = productoOpiniones.filter(filtrarDuplicados)

      //Hacer que clave sean los pares (pelicula1, pelicula2)
      //pasar de
      //	(idUsuario, ((idPelicula1, opinion1), (idPelicula2, opinion2))) a
      //	((idPelicula1, idPelicula2), (opinion1, opinion2))
      val paresPeliculas = opinionesUnicas.map(construirPares)

      //Tenemos ahora (pelicula1, pelicula2) => (opinion1, opinion1)
      //Coleccionar todas las opiniones para cada par de peliculas
      val paresOpionesPeliculas = paresPeliculas.groupByKey()

      //Tenemos (pelicula1, pelicula2) => (opinion1, opinion2), (opinion1, opinion2) ...
      //Calculamos similitudes.
      val similares = paresOpionesPeliculas.mapValues(similitudCoseno).cache()

      //#Salvar los resultados
      similares.sortByKey()
      similares.saveAsTextFile("/loudacre/movie-sims")

      val resultadosFiltrados = similares.filter(t => cumpleCondiciones(t, idPelicula))

      //Ordenar por calidad del resultado. Elegir los 10 mejores
      val resultados = resultadosFiltrados.map({ case( (pair,sim) ) => (sim, pair) }).sortByKey(ascending = false).take(10) //collect()

      val pelSim = diccionarioNombres.filter({case( (m,(m2, m3)) ) => m.toInt == idPelicula}).take(1)

      println("Resultado similares a la pelicula " + pelSim(0))

      resultados.foreach({case((s, p)) => {
        val p1 = diccionarioNombres.filter({case( (m,(m2, m3)) ) => m.toInt == p._1}).take(1)
        val p2 = diccionarioNombres.filter({case( (m,(m2, m3)) ) => m.toInt == p._2}).take(1)

        val mensaje = "%s  similitud: %f  veces: %d".format(p1(0)._2._2,s._1,s._2)
        val mensaje1 = "%s  similitud: %f  veces: %d".format(p2(0)._2._2,s._1,s._2)
        println(mensaje)
        println(mensaje1)
      } })
    } finally {
      sc.stop()
    }
  }
}