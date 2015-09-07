package BigJoin;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.lib.MultipleInputs;



public class MRJoin {

	 public static void main(String[] args) throws Exception {
   	     

		 
		   JobConf conf = new JobConf(MRJoin.class);			 
	  	     conf.setQueueName("ebi");
	  	     conf.setJobName("MRJoin");
	  	     conf.setOutputKeyClass(NullWritable.class);
	  	     conf.setOutputValueClass(Text.class);
	  	
	  	     
	  	    // conf.setInputFormat(OrcInputFormat.class);
	  	     conf.setOutputFormat(TextOutputFormat.class); 
	  	       
	  	     conf.setMapOutputKeyClass(AccountNumID.class);
	  	     conf.setMapOutputValueClass(JoinGenericWritable.class);
	  	    // conf.setOutputKeyComparatorClass(JoinSortingComparator.class);
	  	     //conf.setOutputValueGroupingComparator(JoinGroupingComparator.class);
	  	     
	  	     addJarToDistributedCache(org.apache.hadoop.hive.ql.io.orc.OrcSplit.class,conf);
	  	   conf.set("mapred.input.dir.recursive", "true");
	  	   conf.set("orc.stripe.size", "1073741824");
	  	    
	  	     //Multiple Mapper Class
	  	     MultipleInputs.addInputPath(conf, new Path(args[0]), OrcInputFormat.class,RosettaMap.class);
	  	     MultipleInputs.addInputPath(conf, new Path(args[1]), OrcInputFormat.class,MediumMap.class);
	  	   
	  	     
	  	     //Reducer Class	  	    
	  	     conf.setReducerClass(JoinReducer.class);
	  	     
	  	     conf.setNumReduceTasks(100);
	  	    // conf.setNumMapTasks(5000);
	  	     

	  	     
	  	     FileOutputFormat.setOutputPath(conf, new Path(args[2]));
	  	
	  	     JobClient.runJob(conf);
	  	     
		   
	  }

	
	  private static void addJarToDistributedCache(
		        Class classToAdd, Configuration conf)
		    throws IOException {
		 
		    // Retrieve jar file for class2Add
		    String jar = classToAdd.getProtectionDomain().
		            getCodeSource().getLocation().
		            getPath();
		    File jarFile = new File(jar);
		 
		    // Declare new HDFS location
		    Path hdfsJar = new Path("/user/root/libs/"
		            + jarFile.getName());
		 
		    // Mount HDFS
		    FileSystem hdfs = FileSystem.get(conf);
		 
		    // Copy (override) jar file to HDFS
		    hdfs.copyFromLocalFile(false, true,
		        new Path(jar), hdfsJar);
		 
		    // Add jar to distributed classPath
		    DistributedCache.addFileToClassPath(hdfsJar, conf);
		}
	
}
