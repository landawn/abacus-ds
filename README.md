# abacus-da
Abacus Data Access&Analysis

[![Maven Central](https://img.shields.io/maven-central/v/com.landawn/abacus-da.svg)](https://maven-badges.herokuapp.com/maven-central/com.landawn/abacus-da/)
[![Javadocs](https://www.javadoc.io/badge/com.landawn/abacus-da.svg)](https://www.javadoc.io/doc/com.landawn/abacus-da)


* Matrix: 
[AbstractMatrix](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/AbstractMatrix_view.html), 
[Matrix](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/Matrix_view.html), 
[ByteMatrix](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/ByteMatrix_view.html), 
[IntMatrix](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/IntMatrix_view.html), 
[LongMatrix](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/LongMatrix_view.html), 
[DoubleMatrix](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/DoubleMatrix_view.html)...


* ORMs for NoSQL: 
[MongoCollectionExecutor](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/MongoCollectionExecutor_view.html), 
[CassandraExecutor](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/CassandraExecutor_view.html) with [CQLBuilder](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/CQLBuilder_view.html), 
[CouchbaseExecutor](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/CouchbaseExecutor_view.html), 
[HBaseExecutor](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/HBaseExecutor_view.html), 
[DynamoDBExecutor](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/DynamoDBExecutor_view.html) and 
[Neo4jExecutor](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/Neo4jExecutor_view.html).


* [Run on remote servers without deploying first](https://github.com/landawn/abacus-da/wiki/Deploy-Once,-Run-Anytime):
[RemoteExecutor](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/RemoteExecutor_view.html).


* More: [Sheet](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/Sheet_view.html), [Points](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/Points_view.html), 
[f](https://cdn.staticaly.com/gh/landawn/abacus-da/master/docs/f_view.html), ...


## Download/Installation & [Changes](https://github.com/landawn/abacus-da/blob/master/CHANGES.md):

* [Maven](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.landawn%22)

* Gradle:
```gradle
// JDK 1.8 or above:
compile 'com.landawn:abacus-da:0.9.3'
```


### Functional Programming:
(It's very important to learn Lambdas and Stream APIs in Java 8 to get the best user experiences with the APIs provided in abacus-util)

[What's New in Java 8](https://leanpub.com/whatsnewinjava8/read)

[An introduction to the java.util.stream library](https://www.ibm.com/developerworks/library/j-java-streams-1-brian-goetz/index.html)

[When to use parallel streams](http://gee.cs.oswego.edu/dl/html/StreamParallelGuidance.html)

[Top Java 8 stream questions on stackoverflow](./Top_java_8_stream_questions_so.md)

[Kotlin vs Java 8 on Collection](./Java_Kotlin.md)


## User Guide:
Please refer to [Wiki](https://github.com/landawn/abacus-da/wiki)


## Also See: [abacus-util](https://github.com/landawn/abacus-util).


## Recommended Java programming libraries/frameworks:
[lombok](https://github.com/rzwitserloot/lombok), [Guava](https://github.com/google/guava), [Abacus-StreamEx](https://github.com/landawn/streamex), [Kyro](https://github.com/EsotericSoftware/kryo), [snappy-java](https://github.com/xerial/snappy-java), [lz4-java](https://github.com/lz4/lz4-java), [Caffeine](https://github.com/ben-manes/caffeine), [Ehcache](http://www.ehcache.org/), [Chronicle-Map](https://github.com/OpenHFT/Chronicle-Map), [echarts](https://github.com/apache/incubator-echarts), 
[Chartjs](https://github.com/chartjs/Chart.js), [Highcharts](https://www.highcharts.com/blog/products/highcharts/), [Apache POI](https://github.com/apache/poi)/[easyexcel](https://github.com/alibaba/easyexcel), [mapstruct](https://github.com/mapstruct/mapstruct), [Sharding-JDBC](https://github.com/apache/incubator-shardingsphere), [hppc](https://github.com/carrotsearch/hppc), [fastutil](https://github.com/vigna/fastutil) ...[awesome-java](https://github.com/akullpp/awesome-java)

## Recommended Java programming tools:
[Spotbugs](https://github.com/spotbugs/spotbugs), [JaCoCo](https://www.eclemma.org/jacoco/)...