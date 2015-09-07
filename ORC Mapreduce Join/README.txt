This is an example on how to join two data sets in ORC and write output in text.
There are two sets Medium and Rosetta being join on a field account number
The starting class is MRJoin.java
pom.xml is attached for dependency requirement

An example of setenv for  the program would be like

/root/orc/BigJoin -> Location where *.java is present
/out/put/dir/path -> /db/support/tt2

hc=`hadoop classpath`
export HADOOP_CLASSPATH=/root/orc/BigJoin-v1.jar:/usr/hdp/current/hive-client/lib/hive-exec-0.14.0.2.2.6.4-1.jar:$hc
echo Deleting OutPut Folder
hadoop fs -rm -r -skipTrash /db/support/tt2
echo Compiling
cd /root/orc/BigJoin
javac -cp ..:.:$HADOOP_CLASSPATH *.java
cd /root/orc/
jar -cvf BigJoin-v1.jar BigJoin/
echo Done

Calling command line would be like

yarn jar /root/orc/BigJoin-v1.jar BigJoin.MRJoin /hdfs/path/dataset/1 /hdfs/path/dataset/2 /out/put/dir/path