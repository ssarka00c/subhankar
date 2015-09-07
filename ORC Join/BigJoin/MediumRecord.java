package BigJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MediumRecord implements Writable{

    public Text AccNum = new Text();
    public BytesWritable MediumRec = new BytesWritable();

    public MediumRecord(){}
               
    public MediumRecord(String aId, BytesWritable mRec){
        this.AccNum.set(aId);
        this.MediumRec.set(mRec);
    }

    public void write(DataOutput out) throws IOException {
        this.AccNum.write(out); 
        this.MediumRec.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.AccNum.readFields(in);
        this.MediumRec.readFields(in);
    }
    
    
}
