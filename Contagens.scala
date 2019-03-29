
//import org.apache.spark.{SparkConf, SparkContext} (não é necessário no Databricks)


//Metodo para separar as linhas por campos
def SplitLine(line: String): Either[String, InputLine] = {
    line match {
      case padraoRegEx(host, timestamp, req, codigo, bytes) => Right(InputLine(host, timestamp, req, codigo, bytes.toLong))
      case _ => Left(line)
    }
}


//Cria classe para receber linha separada por campos
case class InputLine(host: String, timestamp: String, req: String, codigo: String, bytes: Long)

	
//Cria padrão regex
val padraoRegEx = """^(\S+) - - \[(.+)\] "(.+)" (\d+) (\S+)""".r	


//Inicializando o Spark (não é necessário no Databricks)
/*val conf = new SparkConf()
conf.setMaster("local")
conf.setAppName("Desafio Semantix")
val sc = new SparkContext(conf)*/
		
//Importa arquivos para um rdd e persistindo na memoria
val input1 = sc.textFile("NASA_access_log_Jul95")
val input2 = sc.textFile("NASA_access_log_Aug95")
val inputRDD = input1.union(input2)
inputRDD.cache()

//Realiza o parse das linhas
val parsedRDD_ini = inputRDD.map(SplitLine(_))

//Filtra apenas linhas com o mesmo padrão do regex
val parsedRDD = parsedRDD_ini.filter(_.isRight).map(_.right.get)


// 1 - Número de hosts únicos
val hostCount = parsedRDD.map(_.host).distinct().count() 		//Contagem distinta do campo "host"
println(s"Número de hosts únicos: ${hostCount}")


// 2 - Total de erros 404
val errorRDD = parsedRDD.filter(_.codigo == "404") 			//Filtra linhas com o campo "codigo" igual a 404
val errorCount = errorRDD.count()					//Realiza a contagem de linhas
println(s"Total de erros 404: ${errorCount}")


// 3 - Os 5 URL's que mais causaram erro 404
val reqsRDD = errorRDD.map(_.req)					//Retorna linhas contendo apaenas o campo com a requisição completa
val urlRDD = reqsRDD.map(_.split(" ")(1))				//Realiza o split da linha separando por espaços	
val urlCount =urlRDD.map(url => (url, 1))				//Retorna quantidade de vezes que cada URL aparece
    .reduceByKey(_ + _)

val topURLs = urlCount.sortBy(_._2, false)				//Retorna as 5 primeiras URLs, ordenando de forma decrescente
    .take(5)

topURLs.foreach { case (k, v) => 
    println(s"URL: $k Contagem: $v") 
}


// 4 - Quantidade de erros 404 por dia
val daysRDD = errorRDD.map(_.timestamp.slice(0, 11))            	//Quantidade de erros por dia
  .map((_, 1))
  .reduceByKey(_ + _)

val daysCount = daysRDD.count()						//Contagem de dias
val mediaErros = errorCount/daysCount					//Média de erros por dia

println(s"Média de erros por dia: ${mediaErros}")
daysRDD.collect().foreach(println)


// 5 - O total de bytes retornados
val totalBytes = parsedRDD.map(_.bytes).reduce(_ + _)			//Soma o total de bytes retornados
println(s"Total de bytes retornados: ${totalBytes}")
