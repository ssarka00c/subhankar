package BigJoin;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

	public class JoinGenericWritable extends GenericWritable {
        
	    private static Class<? extends Writable>[] CLASSES = null;

	    static {
	        CLASSES = (Class<? extends Writable>[]) new Class[] 
	        		{ RosettaRecord.class, MediumRecord.class  };
	    }
	   
	    public JoinGenericWritable() {}
	   
	    public JoinGenericWritable(Writable instance) {
	        set(instance);
	    } 

	    @Override
	    protected Class<? extends Writable>[] getTypes() {
	        return CLASSES;
	    }
	}
