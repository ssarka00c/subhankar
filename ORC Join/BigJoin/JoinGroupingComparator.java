package BigJoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public  class JoinGroupingComparator extends WritableComparator {
    public JoinGroupingComparator() {
        super (AccountNumID.class, true);
    }                             

    @Override 
    public int compare (WritableComparable a, WritableComparable b){
        AccountNumID first = (AccountNumID) a;
        AccountNumID second = (AccountNumID) b;
                      
        return first.accountNum.toString().compareTo(second.accountNum.toString());
        
    }
}

