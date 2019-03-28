# Questões


1. Qual o objetivo do comando cache em Spark?

> O comando `cache()` armazena um conjunto de dados na memória, de forma que a aplicação consiga acessá-lo de forma mais rápida, quando necessário.

2.	O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

> Existem alguns motivos para códigos em Spark serem mais rápidos do que os implementados em MapReduce. 
>
>No caso do MapReduce, após o término de cada job, os dados são escritos em disco. Caso outra operação precise usar o output desse job, ela precisa ler os dados do disco, realizar as operações e escrever no disco novamente. Isso cria uma sobrecarga de I/O no disco e na rede. Já o Spark utiliza a memória, passando os resultados diretamente para as operações subsequentes, eliminando esse gargalo. 
>
> Outro ponto importante é que o Spark mantém uma JVM rodando constantemente em cada nó do sistema, de forma que não é necessário iniciar a execução de tasks toda vez que um novo job é executado, como no caso do MapReduce.

3.	Qual é a função do SparkContext?

> `SparkContext` é a classe responsável por criar os serviços e conexões com o cluster Spark dentro de uma aplicação.

4.	Explique com suas palavras o que é Resilient Distributed Datasets (RDD).

> RDDs são conjuntos de dados (Datasets) que podem estar espalhados por uma ou mais partições (Distributed). Essas partições são tolerantes a falhas (Resilient), podendo ser recalculadas caso algum nó falhe.

5.	GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?

> No `ReduceByKey()`, todos os pares com as mesmas chaves são agrupados ainda em suas respectivas partições antes de passarem pela etapa de shuffling. Já no `GroupByKey()`, todos os pares são enviados individualmente pela rede na etapa de shuffling, causando tráfico desnecessário e ocupando mais espaço na memória.

6.	Explique o que o código Scala abaixo faz.

```
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
	.map(word => (word, 1))
	.reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")
```

> O código carrega um arquivo com algum texto na variável textFile, cria um array separando o texto por palavras e faz uma contagem de quantas vezes cada palavra aparece no texto. No final, as contagens são exportadas para um novo arquivo texto.
