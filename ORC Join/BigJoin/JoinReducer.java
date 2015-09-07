package BigJoin;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.lang.String;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;



public  class JoinReducer   extends MapReduceBase implements Reducer<AccountNumID, JoinGenericWritable, NullWritable, Text> {
    public void reduce(AccountNumID key, Iterator<JoinGenericWritable> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
      
    	//Prepare two list of rosetta rows and medium rows, since they are on the same reducer it would be safe to assume they are of same key
    	List<BytesWritable> lr = new ArrayList<BytesWritable>();
    	List<BytesWritable> lm = new ArrayList<BytesWritable>();
    	
    	while(values.hasNext())
    	{    	
    		Writable Rec =  (Writable) values.next().get(); 
    		if (Rec.getClass().getSimpleName().equalsIgnoreCase("RosettaRecord"))
    		{
    			RosettaRecord r = (RosettaRecord) Rec;    			
    			lr.add(r.RosettaRec);
    			//System.out.println("R Rec Success");
    		}
    		else 
    		{
    			MediumRecord m = (MediumRecord) Rec;
    			lm.add(m.MediumRec);
    			//System.out.println("In M Rec Success");
    		}
    	}
    	
    	int i=0,j=0; 
    	while(i < lr.size())
    	{
    		BytesWritable rRec = lr.get(i);
    		j=0;
    		while(j < lm.size())
    		{
    			
    			BytesWritable mRec =  lm.get(j);
    			
    			try {
    				
    				byte[] rbytes = rRec.copyBytes();
    				byte[] mbytes = rRec.copyBytes();
    				byte[] r = new byte[rbytes.length];
    				byte[] m = new byte[rbytes.length];
    				InputStream ris = new ByteArrayInputStream(rbytes);
                    InputStream mis = new ByteArrayInputStream(mbytes);
                    ris.read(r);
                    mis.read(m);
                      
    				String strR = new String(r);
    				String strM = new String(m);
    				    			     
    			   //Convert the two rows in string and add them side by side for now to save as text    	    	    
    	    		output.collect(NullWritable.get(), new Text(strR + "|" + strM));    	    			
    	    		
    			 } catch (Exception e) {
    			     System.out.println(e);
    			     e.printStackTrace();
    			 }
    			
    			
    			
    			j++;
    		}
    			i++;
    	}
    	
    	
    	
    	
      }
    
    
          
}
  


