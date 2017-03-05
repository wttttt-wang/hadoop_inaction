### Description for repository 'LogCount'
* take a web server log file. Write a MapReduce program to aggregate the number of visits for each IP address. [this is done by the file 'LogCount.java']  [MR1]
* Write another MapReduce program to find the top K IP addresses in terms of visits. [This is done by the file 'Log_Max_k.java'] [MR2]

### Input & Output
* The input of MR1 is a web server(*Nginx*) log file.
* The ouput of MR1 is (IPAddress, VisitNum) in each line. [This is also the input of MR2]
* The output of MR2 is the topK visits IP, format as (IPAddress, VisitNum) in each line.

### source file
* 'LogCount.java' and 'Log_Max_k.java'

### How to jar and run the project
eg:
1. javac LogCount.java
2. jar  -cvf  LogCount.jar  ./LogCount*
3. hadoop  jar  LogCount.jar  LogCount  /input  /output
* For more detail, see [http://blog.csdn.net/u014265088/article/details/60133106](http://blog.csdn.net/u014265088/article/details/60133106 "Linux下打包运行MR程序")

### For 'Log_Max_k.py'
* the same function as 'Log_Max_k.java', but it's stand-alone(Do not run it in hadoop)
* For me, it is used to check the correctness of MR2.

### Usage
* hadoop jar LogCount.jar LogCount InputDir OutputDir
* hadoop jar Log_Max_k.jar Log_Max_k k InputDir OutputDir