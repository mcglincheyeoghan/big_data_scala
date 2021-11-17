# big_data_scala
This was the second assignment I did as part of the Programming for Big Data module. In this assignment, I analysed data using
Scala and Spark. I analysed the data useing various SQL commands using Spark to execute various queries and then I processed 
graphs using Spark. The data analysis can be viewed in the question1.sc file and the graph processing can be viewed in the 
question2.sc file.
To run the scala files in cmd, first, copy the file to the docker container
docker cp <filePath> <containerId>:/
Once you are executing the container, execute the file using
spark/bin/spark-shell -i <filePath>
