



 bin/spark-shell --master yarn-client  --executor-memory 2g


## Create RDD


### In-memory data

    val lines = List("hello how are you","i am fine how are you?")
    val rdd = sc.makeRDD(lines)
    rdd.collect()

### From file

    val fileRDD = sc.textFile("README.md")
    fileRDD.collect()


    val fileRDD = sc.textFile("/input/sales.csv")
    fileRDD.collect()
   
    val fileRDD2 = sc.textfile("/input/wc.txt")

    val concomp = sc.textFile("/input/Consumer_Complaints.csv") 

## Word Count example

    val words = fileRDD.flatMap(line => line.split("\\s+"))
    val wordsMapping = words.map(word => (word,1))   // Pair
    val result = wordsMapping.reduceByKey((a,b) => a+b) // Shuffle RDD
    result.collect()

   
    val words = fileRDD.flatMap(line => line.split(","))
    val wordsMapping = words.map(word => (word,1))
    val result = wordsMapping.reduceByKey((a,b) => a+b)
    result.collect() 
 
## Types of RDD

  * MappedRDD

  * FlatMappedRDD

  * Shuffle RDD

  * Double RDD

  etc


## Double RDD example

    val doubleList = List(1.0,2.0,4.0,5.0)
    val doubleRDD = sc.makeRDD(doubleList)
    doubleRDD.sum

    /*
       This will give error
       fileRDD.sum
    */


## Filtering

    val sparkLines = fileRDD.filter(line => line.contains("spark"))

    val sparkLines = fileRDD.filter(line => line.contains("4"))

## Count

    sparkLines.count()




## Key,Value pair operations

    val salesRDD = sc.textFile("/input/sales.csv")
    val customerPair = salesRDD.map(line => {
       val cols = line.split(",")
       ( cols(1),  cols(3).toDouble)
    })



### countByKey

    customerPair.countByKey


### groupByKey

    val groupByCustomer = customerPair.groupByKey
    groupByCustomer.collect()


### lookUp

    val lookUpCustomer = customerPair.lookup("1")


### joins

    val customerRDD = sc.textFile("/input/customers.csv")
    val customerInfo = customerRDD.map(value => {
      val cols = value.split(",")
     (cols(0), cols(1))
    })

    val joinedRDD = customerPair.join(customerInfo)



## Caching

    customerRDD.cache()

    //Removing underneath file customers.csv

    customerRDD.collect()



