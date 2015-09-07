package BigJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class AccountNumID implements WritableComparable{

	public static final IntWritable ROSETTA_RECORD = new IntWritable(0);
	public static final IntWritable MEDIUM_RECORD = new IntWritable(1);
	
	public Text accountNum= new Text();
	public IntWritable sourceId = new IntWritable();
	
	//Empty Constructor Needed For Hadoop to Call
	public AccountNumID() {}
	
	//Assign
	public AccountNumID(Text aNum, IntWritable sId )
	{
		this.accountNum = aNum;
		this.sourceId = sId;
	}
	
	public void write(DataOutput out) throws IOException 
	{
		this.accountNum.write(out);
		this.sourceId.write(out);
	}
	
	public void readFields(DataInput in) throws IOException
	{
		this.accountNum.readFields(in);
		this.sourceId.readFields(in);
	}
	
	
	public boolean equals (AccountNumID other) {
	    return this.accountNum.equals(other.accountNum) && this.sourceId.equals(other.sourceId );
	}

	public int hashCode() {
	    return this.accountNum.hashCode();
	}

	@Override
	public int compareTo(Object arg0) {
		
		AccountNumID aid ;
		aid = (AccountNumID) arg0;
		
		return this.accountNum.compareTo(aid.accountNum);
	    
	} 
}


	
	

