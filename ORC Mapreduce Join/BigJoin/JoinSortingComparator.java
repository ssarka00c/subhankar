package BigJoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public  class JoinSortingComparator extends WritableComparator {
    public JoinSortingComparator()
    {
        super (AccountNumID.class, true);
    }
                                
    @Override
    public int compare (WritableComparable a, WritableComparable b){
        AccountNumID first = (AccountNumID) a;
        AccountNumID second = (AccountNumID) b;
                                 
        return first.compareTo(second);
    }
}
