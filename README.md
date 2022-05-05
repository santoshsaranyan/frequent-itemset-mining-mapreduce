
# Frequent Itemset Mining 

Spring 2022

Description
-----------
This project focuses on creating a parallel version of the Apriori algorithm for frequent itemset mining, and uses this algorithm to analyze H&M transaction data to find items frequently bought together.

• Developed in MapReduce, a parallel implementation of the Apriori Algorithm for frequent itemset mining.

• Used clusters in AWS EMR to run the frequent itemset algorithm, and AWS S3 to store the data.

• Analyzed clothing retailer data to find products that were frequently bought together.

Source for the dataset: https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/overview 

My contributions to this project include apriorigen.java (Code to generate candidate itemsets for a given iteration), dataprep.java (Code to preprocess the dataset for use with our algorithm), and help with coming up with the pseudo code for the whole algorithm.

Code authors
-----------
Santosh Saranyan

Nihal Desai

Selen Toptas

Installation
------------
These components are installed:
- JDK 1.8
- Hadoop 3.2.2
- Maven
- AWS CLI (for EMR execution)

Environment
-----------
1) Example ~/.bash_aliases:
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home
export HADOOP_HOME=/Users/santoshsaranyan/myhadoop/hadoop-3.2.2
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

2) Explicitly set JAVA_HOME in $HADOOP_HOME/etc/hadoop/hadoop-env.sh:
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	make switch-standalone		-- set standalone Hadoop environment (execute once)
	make local
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	make switch-pseudo			-- set pseudo-clustered Hadoop environment (execute once)
	make pseudo					-- first execution
	make pseudoq				-- later executions since namenode and datanode already running 
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	make upload-input-aws		-- only before first execution
	make aws					-- check for successful execution with web interface (aws.amazon.com)
	download-output-aws			-- after successful execution & termination
