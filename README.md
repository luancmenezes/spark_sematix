# Desafio Spark - Semantix

## Qual o objetivo do comando cache em Spark?

O objetivo do comando .cache() é a otimização de jobs Spark, onde em um contexto de BigData o desafio comum é o uso demasiado da memória, visto que operações de execução longa e tarefas que resultam em operações Cartesianas.  No Spark existem dois tipos de operações sobre RDD Transforsparko e Ação. As transformações criam um RDD em outro RDD e têm a característica de ser "lazy", isso significa que serão computadas somente quando ação for aplicada no RDD. Entretanto, as ações são operações sobre o RDD, dessa vez não cria um novo  RDD mas agregam os elementos do RDD  usando alguma função e retorna ao driver o resultado final. 

Em um  RDD que não é utilizado o método .cache() o DAG criado pelas transformações será sempre executado a cada nova ação executada no RDD, ocasionado em um retrabalho das execuções. Assim, usando o comando cache o Spark manterá o RDD na memória, tornando as operações mais rápidas.

## O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

Ambos os frameworks foram implementados para execução de jobs paralelos e em cluster. Porém o Spark realiza processamento na memória principal dos nós do cluster, impedindo as operações de leitura e escrita desnecessárias com os discos. A outra vantagem oferecida pelo Spark é a capacidade de encadear as tarefas através do uso grafo acíclico direcionado (DAG) como engine de processamento de dados. Assim, para cada job um DAG de diferentes estágios de tarefas é criado para ser executado em um cluster, minimizando o número de gravações nos discos. Enquanto o Hadoop MapReduce a sua “DAG” consiste em apenas dois estágios Map e Reduce, não podendo acumular diversas transformações, ocasionando em uma necessidade grande de leitura e escrita de arquivos no HDFS, tornando suas execuções mais lentas do que o Spark.

## Qual é a função do SparkContext?

O SparkContext representa a conexão inicial com o Spark cluster. Após a criação do contexto é possível criar RDDs, serviços do Spark e realizar tarefas. Ele é responsável por configurar o ambiente de execução e estabelecer a conexão com o ambiente de execução do Spark.

## Explique com suas palavras o que é Resilient Distributed Datasets (RDD).

RDD (Resilient Distributed semantixset)  é uma estrutura de dados desenvolvida pelo Apache Spark, no qual é uma coleção imutável  de objetos, que são computadas em diferentes nós do cluster. Todas as vezes que o semantixset é carregado e transformados em um RDD o mesmo é logicamente particionado e computados por diferentes nós. Por exemplo: ``["semantix","bigData","semantix","semantix","spark","semantix","bigData","spark"]  `` obter um RDD com as seguintes 4 partições ``[['semantix', 'bigData','semantix','semantix'],['spark', 'semantix', bigData, 'spark']]`` , onde cada partição pode ser distribuída para diferentes nós de um cluster e ser processada paralelamente.
 
## GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?

Com o groupByKey há uma grande quantidade de dados para serem transferidos pela rede desnecessariamente, além disto, operações com grandes dataset o uso do GroupByKey altera o modo de operação e passa salvar dados no disco, pelo fato de haver mais memória no shuffling do que na memória do executor.

Exemplo :
~~~~ 
val data = spark.sparkContext.parallelize(Array(('k',5),('s',3),('s',4),('p',7),('p',5),('t',8),('k',6)),3)
val group = data.groupByKey().collect()
group.foreach(println)
~~~~

Entretanto o reduceByKey, existe uma combinação de de chaves identicas é uma partição do cluster, diminuindo a quantidade de dafos, diminuindo o consumo da memória.

Exemplo:
```
val words = Array("one","two","two","four","five","six","six","eight","nine","ten")
val data = spark.sparkContext.parallelize(words).map(w => (w,1)).reduceByKey(_+_)
data.collect.foreach(println)
```

## Explique o que o código Scala abaixo faz.

```
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")
```

A linha: ``val textFile = sc.textFile("hdfs://...")`` cria um RDD a partir de um arquivo de texto armazenado no hdfs.
Exemplo: A partir de arquivo de texto contendo ``spark semantix spark spark sp spark spark sp approved`` teríamos como resultado o seguinte RDD ``[['spark semantix spark spark sp spark spark sp approved']]``

A linha ``val counts = textFile.flatMap(line => line.split(" "))`` transforma o RDD textFile aplicando a função flatMap, que retorna um array com uma posição e esta linha é submetida a função split(), que separa essas linha por espaço. O resultado final é um novo RDD com as palavras separadas 
``[['spark','semantix','spark','spark','sp','spark','spark','sp','approved']]``

A linha ``.map(word => (word, 1))`` aplica a função passada como parâmetro da transformando map em cada elemento do RDD resultante do flatMap, produzindo um novo RDD que contém um conjunto de tuplas.
``[[('spark', 1), ('semantix', 1), ('spark', 1), ('spark', 1), ('sp', 1), ('spark', 1), ('spark', 1), ('sp', 1),('approved', 1)]]``

A linha ``.reduceByKey(_ + _)`` aplica uma ação no RDD acima, a qual combina os elementos com a mesma palavra chave somando os valores e por fim retornando a variável counts um novo RDD como mostrado abaixo que representa a quantidade ocorrências de cada palavra no texto original.
``[[('semantix', 1)], [('spark', 5), ('sp', 2), ('approved', 1)]]``

Finalmente, a linha ``counts.saveAsTextFile("hdfs://...")`` salva o RDD counts como um arquivo de texto no hdfs.







