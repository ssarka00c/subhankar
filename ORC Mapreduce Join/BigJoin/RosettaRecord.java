package BigJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class RosettaRecord implements Writable{

    public Text AccountId = new Text();
    public BytesWritable RosettaRec = new BytesWritable();

    public RosettaRecord(){}
               
    public RosettaRecord(String aId, BytesWritable rRec){
        this.AccountId.set(aId);
        this.RosettaRec.set(rRec);
    }

    public void write(DataOutput out) throws IOException {
        this.AccountId.write(out);
        this.RosettaRec.write(out); 
    }

    public void readFields(DataInput in) throws IOException {
        this.AccountId.readFields(in);
        this.RosettaRec.readFields(in);
    }
    
    
}
