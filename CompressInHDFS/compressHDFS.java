package hdfsInteract;


import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//http://princetonits.com/technology/using-filesystem-api-to-read-and-write-data-to-hdfs/
//http://blog.rajeevsharma.in/2009/06/using-hdfs-in-java-0200.html


public class compressHDFS {
	
	private static String workDir="C:\\hdfs";
	private static final String FS_PARAM_NAME = "fs.defaultFS";
	
	@SuppressWarnings("deprecation")
	public static void main( String[] args )
	{
		
		if(args.length == 0)
		{
			System.out.print("The Command Needs Atleast One Argument, that is teh absolute file path to be complressed");
			System.exit(1);
		}
		String fPath=args[0].trim();
		String tfPath=fPath + ".bz2";
		
		try
		{
		System.setProperty("HADOOP_USER_NAME", "root");		
		Configuration conf = new Configuration();
	    conf.addResource(new Path(workDir + "\\core-site.xml"));
	    conf.addResource(new Path(workDir + "\\hdfs-site.xml"));
	    conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	    conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
	    FileSystem fs = FileSystem.get(conf);
	    
	    
	    Path path = new Path(fPath);
	    //Path target = new Path("/user/root/rt.data.bz2");
	    
	    if (fs.exists(path)) {
	        System.out.println("File Exists On HDFS");
	        
	    }
	    else
	    {
	    	System.out.println("File Does Not Exists Or You Do Not Have Access To The File");
	    	System.exit(1);
	    }
	    
	    long fileSize = fs.getLength(path);
	    System.out.println("Lenth : " + fileSize);
	    long bw = 524288;
	    
	    
	    FSDataInputStream in = fs.open(path);
	    String fileName = path.getName();
	    //Path target = new Path (fileName + ".bz2");
	    Path target = new Path (tfPath);
	    if(fs.exists(target))
	    {
	    	System.out.print("File " + target.getName() + " already exists... Deleting it for rewrite");
	    	fs.delete(target, true);
	    }
	    
	    
	  try
	    {
	    	
	    FSDataOutputStream cfile=fs.create(target);
	    //GzipCodec gzipCodec=(GzipCodec)ReflectionUtils.newInstance(GzipCodec.class,conf);
	    //Compressor gzipCompressor=CodecPool.getCompressor(gzipCodec);
	    BZip2Codec bz2 = (BZip2Codec)ReflectionUtils.newInstance(BZip2Codec.class,conf);
	    Compressor bzip2Compressor=CodecPool.getCompressor(bz2);
	    OutputStream compressedOut=bz2.createOutputStream(cfile,bzip2Compressor);
	    
	    System.out.print("Starting To Compress Bz2....");
	    try {
	        IOUtils.copyBytes(in,compressedOut,conf);
	        System.out.println("Compress Done...");
	      }
	     catch (  Exception e) {
	        System.out.println("Error in compressing :" + e.getMessage());
	        e.printStackTrace();
	      }
	     finally {
	    	 
	    	 cfile.flush();	    	 
	    	 cfile.close();
	    	 in.close();
	        CodecPool.returnCompressor(bzip2Compressor);
	        //compressedOut.close();
	        fs.close();
	      }
	    
	    }
	    catch(Exception ex)
	    {
	    	System.out.println("Exception in new block" + ex.getMessage());
	    	ex.printStackTrace();
	    }
	    ////  
	    

	    
	    
	    
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	    
		
	}

}
