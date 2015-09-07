package BigJoin;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


class MediumMap extends  MapReduceBase implements Mapper<NullWritable, OrcStruct , AccountNumID, JoinGenericWritable> {
 
      public void map(NullWritable key, OrcStruct value, OutputCollector<AccountNumID, JoinGenericWritable> output, Reporter reporter) throws IOException 
      {
    	  int numOfFields = 284;
    	  //It will read OrcStruct, extract Account Number from OrcStruc and Carry forward Account Numb as key and OrcStruc As value
    	  Properties properties = new Properties();
    	   value.setNumFields(numOfFields);
		  
    	  properties.setProperty("columns", "c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,c34,c35,c36,c37,c38,c39,c40,c41,c42,c43,c44,c45,c46,c47,c48,c49,c50,c51,c52,c53,c54,c55,c56,c57,c58,c59,c60,c61,c62,c63,c64,c65,c66,c67,c68,c69,c70,c71,c72,c73,c74,c75,c76,c77,c78,c79,c80,c81,c82,c83,c84,c85,c86,c87,c88,c89,c90,c91,c92,c93,c94,c95,c96,c97,c98,c99,c100,c101,c102,c103,c104,c105,c106,c107,c108,c109,c110,c111,c112,c113,c114,c115,c116,c117,c118,c119,c120,c121,c122,c123,c124,c125,c126,c127,c128,c129,c130,c131,c132,c133,c134,c135,c136,c137,c138,c139,c140,c141,c142,c143,c144,c145,c146,c147,c148,c149,c150,c151,c152,c153,c154,c155,c156,c157,c158,c159,c160,c161,c162,c163,c164,c165,c166,c167,c168,c169,c170,c171,c172,c173,c174,c175,c176,c177,c178,c179,c180,c181,c182,c183,c184,c185,c186,c187,c188,c189,c190,c191,c192,c193,c194,c195,c196,c197,c198,c199,c200,c201,c202,c203,c204,c205,c206,c207,c208,c209,c210,c211,c212,c213,c214,c215,c216,c217,c218,c219,c220,c221,c222,c223,c224,c225,c226,c227,c228,c229,c230,c231,c232,c233,c234,c235,c236,c237,c238,c239,c240,c241,c242,c243,c244,c245,c246,c247,c248,c249,c250,c251,c252,c253,c254,c255,c256,c257,c258,c259,c260,c261,c262,c263,c264,c265,c266,c267,c268,c269,c270,c271,c272,c273,c274,c275,c276,c277,c278,c279,c280,c281,c282,c283");
		  properties.setProperty("columns.types", "string,string,string,string,string,bigint,bigint,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,timestamp,timestamp,timestamp,timestamp,decimal(38,10),timestamp,timestamp,decimal(38,10),string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,decimal(38,10),decimal(38,10),timestamp,timestamp,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,timestamp,timestamp,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,timestamp,timestamp,string,timestamp,timestamp,string,string,string,string,string,int,string,string,string,string,int,string,string,string,string,timestamp,string,string,string,string,string,string,string,string,string,string,string,string,timestamp,timestamp,string,string,string,string,timestamp,timestamp,string,string,string,string,string,timestamp,timestamp,string,string,string,string,string,string,string,timestamp,timestamp,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,timestamp,timestamp,string,timestamp,timestamp,string,string,string,string,string,int,string,string,string,int,string,string,string,string,string,int,int,int,string,string,string,string,string,string,string,string,string,string,int,int,int,string,string,string,string,string,string,string,string,string,timestamp,timestamp,string,timestamp,timestamp,string,int,string,string,string,string,string,int,string,string,string,string,string,string,int,string,string,string,timestamp,string,string,string,string,string,string,string,string,timestamp,timestamp,string,timestamp,timestamp,string,string,string,string,string,string,string,timestamp,timestamp,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,timestamp,string,string");
		  		  
		  BytesWritable vblob = null;  // will contain value orc in byteswritable
		  BinarySortableSerDe vserde = null; // Will be used to converting value orc to byteswritabel
		  		  
		  SerDe serde = new OrcSerde();
    	  try {
				serde.initialize(new Configuration(), properties);
			} catch (SerDeException e) {
				// TODO Auto-generated catch block
				System.out.println("serde.initialize");
				e.printStackTrace();
			}
    	  
    	  
    	  Object fields=null;
		try {
			fields = serde.deserialize(value);
		} catch (SerDeException e1) {
			// TODO Auto-generated catch block
			System.out.println("fields=serde.deserialize");
			e1.printStackTrace();
		}
    	  String keystr="";
    	  int keyIndex=0;
    	  String txtRow = "";
    	  
    	  StructObjectInspector in=null;
			try {
				in = (StructObjectInspector) serde.getObjectInspector();
				List <Object> vals = in.getStructFieldsDataAsList(fields);
				
				try {
					keystr = vals.get(keyIndex).toString();
				} catch(Exception ex) {keystr = "";}
				String row="";
				
				for (int k=0;k<vals.size();k++)
				{
					try {
					row=row + vals.get(k).toString() + "|"; } catch(Exception e2) { row=row + "|"; }
				}
				
				if(row.length() > 2)
				{
				row=row.substring(0,row.length() - 2);
				
				 try {					
                     vblob = new BytesWritable(row.getBytes());
                     
				 } catch (Exception ex) {}
				
				}
				// else
					//System.out.println("BadRow=" + row);
				
			} catch (SerDeException e) {
				// TODO Auto-generated catch block
				keystr="";
				System.out.println("R keystr=null");
				e.printStackTrace();
			}
			
			if(keystr != "" && vblob !=null) 
			{
		
			AccountNumID mKey = new AccountNumID(new Text(keystr),AccountNumID.MEDIUM_RECORD); 
			MediumRecord mRecord = new MediumRecord(keystr,vblob);
			JoinGenericWritable mVal = new JoinGenericWritable(mRecord); 
			output.collect(mKey, mVal);
			}
    		  
      }
    	 
  
  }
