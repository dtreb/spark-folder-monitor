#Spark streaming as folder monitor

###Info
This is an example that demonstrates usage of:

 1. [Spark streaming](http://spark.apache.org/streaming/) and embedded Spark instance.   
 2. Different spark data analysis approaches.        
 3. [Commons CLI](https://commons.apache.org/proper/commons-cli/) to parse parameters.   

###Run

 1. Build project with ```$ mvn package```.   
 2. Locate created **spark-folder-monitor-xxx-jar-with-dependencies.jar** in target folder.
 3. Run ```$ java -jar spark-folder-monitor-xxx-jar-with-dependencies.jar --help``` to get information about available parameters.   
 4. Run ```$ java -jar spark-folder-monitor-xxx-jar-with-dependencies.jar``` to use default parameters (check [monitor](./monitor) folder, display 10 items, use 10 seconds interval).    
 5. Move some text files to monitored folder.   
 6. Check console output. You should see analysis results - top used words, longest line etc.      
 
Feel free to use, comment or collaborate. 
     
