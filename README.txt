//Shashank Gupta 800970543

src : PageRank.java, GraphLink.java, WikiCount.java, PageRankInitializer.java, PageRankAlgo.java and Sorter.java 
out : wikimicro-out.txt and simplewiki-out.txt

******************************************************************************************************************************

--> Description of classes:

PageRank.java
Driver class which calls other classes for computing the PageRank.

GraphLink.java
GraphLink generates the title and its list of outgoing links

WikiCount.java
Counts the number of lines/pages

PageRankInitializer.java
Initialize page rank score with 1/N

PageRankAlgo.java
PageRank computation using weightage/contribution.
Map output 	<Title>, <!> 	for titles whose pagerank needs to be computed

Sorter.java
Sorts the titles by PageRank.


****************************************************************************************************************************************

********************************************************************************************************************************
Commands:

--> Running on Cloudera VM

1. Create directories in hadoop
hadoop fs -mkdir /user/cloudera/as3 /user/cloudera/as3/input 

2. Copy input files to hadoop
hadoop fs -copyFromLocal /home/cloudera/a3/input/* /user/cloudera/as3/input

3. Create directory build
mkdir -p build

4. Compile all java files using *.java
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint

5. Create jar pagerank.jar 
jar -cvf pagerank.jar -C build/ .

6. Run the PageRank class and pass the input and output paths
hadoop jar pagerank.jar org.myorg.PageRank /user/cloudera/as3/input /user/cloudera/as3/o1

7. Copy the output to local directory by using the following command 
hadoop fs -copyToLocal /user/cloudera/as3/o1/* /home/cloudera/a3/output



--> Running SimpleWiki on cluster

1. Used the following scp command to copy files to cluster
scp PageRank.java sgupta27@dsba-hadoop.uncc.edu:/users/sgupta27/shashank

2. Connecting to the cluster
ssh -X sgupta27@dsba-hadoop.uncc.edu

3. Go to the directory of /projects/cloud/pagerank to copy the file to hadoop
hadoop fs ­put simplewiki* /user/sgupta27/i1

4. Go to the directory where you have kept the java files. In my case it was 'shashank'.
cd ..

5. Create directory build
mkdir build

6. Compile all java files using *.java
javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build -Xlint

7. Create jar pagerank.jar 
jar -cvf pagerank.jar -C build/ .

8. Run the PageRank class and pass the input and output paths
hadoop jar pagerank.jar org.myorg.PageRank /user/sgupta27/i1 /user/sgupta27/o1

9. Merging the output files generated
hadoop fs -getmerge /user/sgupta27/o1/part­r* outputfile

10. Copy the output from hadoop to your cluster
hadoop fs -copyToLocal /user/sgupta27/o1/* /users/sgupta27/shashank/output

11. Copy the outputfile to your local system
scp sgupta27@dsba-hadoop.uncc.edu:/users/sgupta27/shashank/outputfile .

********************************************************************************************************************************************











