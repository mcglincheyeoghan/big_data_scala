val data = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("github-big-data.csv")
data.registerTempTable("dataTable")

//question 1
val mostStars = spark.sql("select name, stars from dataTable where stars = (select max(stars) from dataTable)")
println("question 1:")
mostStars.collect.foreach(println)
println

//question 2
val totalStarsPerLanguage = spark.sql("select language, sum(stars) from dataTable group by language")
println("question 2:")
totalStarsPerLanguage.collect.foreach(println)
println

//question 3a
val numberDescriptionsdata = spark.sql("select count(description) from dataTable where description regexp 'data'")
println("question 3a:")
numberDescriptionsdata.collect.foreach(println)
println 

//question 3b
val numberDescriptionsdataNonNullLanguage = spark.sql("select count(description) from dataTable where (description regexp 'data' and language is not null)")
println("question 3b:")
numberDescriptionsdataNonNullLanguage.collect.foreach(println)
println

//question 4
println("question 4:")
//select the description column from the dataTable and assign it to the Description variable
val Description = spark.sql("select description from dataTable")
//move the Description variable to a temporary csv file, setting the header to false since the select statement won't return headers
Description.write.format("com.databricks.spark.csv").option("header", "false").save("descriptions.csv")
//convert the temporary csv file to a txt file 
val descriptionToText = sc.textFile("descriptions.csv")
//use MapReduce to count the number of occurrences of each word in the txt file
val wordCount = descriptionToText.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
//swap the key and value for each pair and assign it to the swap_wordCount variable
val swap_wordCount = wordCount.map(_.swap)
//sort the keys and value pairs in swap_wordCount in descending order (ascending = false) and assign it to the mostFrequentWordFromDescription variable
val mostFrequentWordFromDescription = swap_wordCount.sortByKey(false, 1)
//take the key and value pair from mostFrequentWordFromDescription
val mostFrequent = mostFrequentWordFromDescription.take(1)
//convert the array to an RDD
val mostFrequentRDD = sc.parallelize(mostFrequent)
mostFrequentRDD.collect.foreach(println)

System.exit(0)