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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public  class RosettaMap extends  MapReduceBase implements Mapper<NullWritable, OrcStruct , AccountNumID, JoinGenericWritable> {

      public void map(NullWritable key, OrcStruct value, OutputCollector<AccountNumID, JoinGenericWritable> output, Reporter reporter) throws IOException
      {

          //It will read OrcStruct, extract Account Number from OrcStruc and Carry forward Account Numb as key and OrcStruc As value
          Properties properties = new Properties();
          
    
                        properties.setProperty("columns", "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20,c21,c22,c23,c24,c25,c26,c27,c28,c29,c30,c31,c32,c33,c34,c35,c36,c37,c38,c39,c40,c41,c42,c43,c44,c45,c46,c47,c48,c49,c50,c51,c52,c53,c54,c55,c56,c57,c58,c59,c60,c61,c62,c63,c64,c65,c66,c67,c68,c69,c70,c71,c72,c73,c74,c75,c76,c77,c78,c79,c80,c81,c82,c83,c84,c85,c86,c87,c88,c89,c90,c91,c92,c93,c94,c95,c96,c97,c98,c99,c100,c101,c102,c103,c104,c105,c106,c107,c108,c109,c110,c111,c112,c113,c114,c115,c116,c117,c118,c119,c120,c121,c122,c123,c124,c125,c126,c127,c128,c129,c130,c131,c132,c133,c134,c135,c136,c137,c138,c139,c140,c141,c142,c143,c144,c145,c146,c147,c148,c149,c150,c151,c152,c153,c154,c155,c156,c157,c158,c159,c160,c161,c162,c163,c164,c165,c166,c167,c168,c169,c170,c171,c172,c173,c174,c175,c176,c177,c178,c179,c180,c181,c182,c183,c184,c185,c186,c187,c188,c189,c190,c191,c192,c193,c194,c195,c196,c197,c198,c199,c200,c201,c202,c203,c204,c205,c206,c207,c208,c209,c210,c211,c212,c213,c214,c215,c216,c217,c218,c219,c220,c221,c222,c223,c224,c225,c226,c227,c228,c229,c230,c231,c232,c233,c234,c235,c236,c237,c238,c239,c240,c241,c242,c243,c244,c245,c246,c247,c248,c249,c250,c251,c252,c253,c254,c255,c256,c257,c258,c259,c260,c261,c262,c263,c264,c265,c266,c267,c268,c269,c270,c271,c272,c273,c274,c275,c276,c277,c278,c279,c280,c281,c282,c283,c284,c285,c286,c287,c288,c289,c290,c291,c292,c293,c294,c295,c296,c297,c298,c299,c300,c301,c302,c303,c304,c305,c306,c307,c308,c309,c310,c311,c312,c313,c314,c315,c316,c317,c318,c319,c320,c321,c322,c323,c324,c325,c326,c327,c328,c329,c330,c331,c332,c333,c334,c335,c336,c337,c338,c339,c340,c341,c342,c343,c344,c345,c346,c347,c348,c349,c350,c351,c352,c353,c354,c355,c356,c357,c358,c359,c360,c361,c362,c363,c364,c365,c366,c367,c368,c369,c370,c371,c372,c373,c374,c375,c376,c377,c378,c379,c380,c381,c382,c383,c384,c385,c386,c387,c388,c389,c390,c391,c392,c393,c394,c395,c396,c397,c398,c399,c400,c401,c402,c403,c404,c405,c406,c407,c408,c409,c410,c411,c412,c413,c414,c415,c416,c417,c418,c419,c420,c421,c422,c423,c424,c425,c426,c427,c428,c429,c430,c431,c432,c433,c434,c435,c436,c437,c438,c439,c440,c441,c442,c443,c444,c445,c446,c447,c448,c449,c450,c451,c452,c453,c454,c455,c456,c457,c458,c459,c460,c461,c462,c463,c464,c465,c466,c467,c468,c469,c470,c471,c472,c473,c474,c475,c476,c477,c478,c479,c480,c481,c482,c483,c484,c485,c486,c487,c488,c489,c490,c491,c492,c493,c494,c495,c496,c497,c498,c499,c500,c501,c502,c503,c504,c505,c506,c507,c508,c509,c510,c511,c512,c513,c514,c515,c516,c517,c518,c519,c520,c521,c522,c523,c524,c525,c526,c527,c528,c529,c530,c531,c532,c533,c534,c535,c536,c537,c538,c539,c540,c541,c542,c543,c544,c545,c546,c547,c548,c549,c550,c551,c552,c553,c554,c555,c556,c557,c558,c559,c560,c561,c562,c563,c564,c565,c566,c567,c568,c569,c570,c571,c572,c573,c574,c575,c576,c577,c578,c579,c580,c581,c582,c583,c584,c585,c586,c587,c588,c589,c590,c591,c592,c593,c594,c595,c596,c597,c598,c599,c600,c601,c602,c603,c604,c605,c606,c607,c608,c609,c610,c611,c612,c613,c614,c615,c616,c617,c618,c619,c620,c621,c622,c623,c624,c625,c626,c627,c628,c629,c630,c631,c632,c633,c634,c635,c636,c637,c638,c639,c640,c641,c642,c643,c644,c645,c646,c647,c648,c649,c650,c651,c652,c653,c654,c655,c656,c657,c658,c659,c660,c661,c662,c663,c664,c665,c666,c667,c668,c669,c670,c671,c672,c673,c674,c675,c676,c677,c678,c679,c680,c681,c682,c683,c684,c685,c686,c687,c688,c689,c690,c691,c692,c693,c694,c695,c696,c697,c698,c699,c700,c701,c702,c703,c704,c705,c706,c707,c708,c709,c710,c711,c712,c713,c714,c715,c716,c717,c718,c719,c720,c721,c722,c723,c724,c725,c726,c727,c728,c729,c730,c731,c732,c733,c734,c735,c736,c737,c738,c739,c740,c741,c742,c743,c744,c745,c746,c747,c748,c749,c750,c751,c752,c753,c754,c755,c756,c757,c758,c759,c760,c761,c762,c763,c764,c765,c766,c767,c768,c769,c770,c771,c772,c773,c774,c775,c776,c777,c778,c779,c780,c781,c782,c783,c784,c785,c786,c787,c788,c789,c790,c791,c792,c793,c794,c795,c796,c797,c798,c799,c800,c801,c802,c803,c804,c805,c806,c807,c808,c809,c810,c811,c812,c813,c814,c815,c816,c817,c818,c819,c820,c821,c822,c823,c824,c825,c826,c827,c828,c829,c830,c831,c832,c833,c834,c835,c836,c837,c838,c839,c840,c841,c842,c843,c844,c845,c846,c847,c848,c849,c850,c851,c852,c853,c854,c855,c856,c857,c858,c859,c860,c861,c862,c863,c864,c865,c866,c867,c868,c869,c870,c871,c872,c873,c874,c875,c876,c877,c878,c879,c880,c881,c882,c883,c884,c885,c886,c887,c888,c889,c890,c891,c892,c893,c894,c895,c896,c897,c898,c899,c900,c901,c902,c903,c904,c905,c906,c907,c908,c909,c910,c911,c912,c913,c914,c915,c916,c917,c918,c919,c920,c921,c922,c923,c924,c925,c926,c927,c928,c929,c930,c931,c932,c933,c934,c935,c936,c937,c938,c939,c940,c941,c942,c943,c944,c945,c946,c947,c948,c949,c950,c951,c952,c953,c954,c955,c956,c957,c958,c959,c960,c961,c962,c963,c964,c965,c966,c967,c968,c969,c970,c971,c972,c973,c974,c975,c976,c977,c978,c979,c980,c981,c982,c983,c984,c985,c986,c987,c988,c989,c990,c991,c992,c993,c994,c995,c996,c997,c998,c999,c1000,c1001,c1002,c1003,c1004,c1005,c1006,c1007,c1008,c1009,c1010,c1011,c1012,c1013,c1014,c1015,c1016,c1017,c1018,c1019,c1020,c1021,c1022,c1023,c1024,c1025,c1026,c1027,c1028,c1029,c1030,c1031,c1032,c1033,c1034,c1035,c1036,c1037,c1038,c1039,c1040,c1041,c1042,c1043,c1044,c1045,c1046,c1047,c1048,c1049,c1050,c1051,c1052,c1053,c1054,c1055,c1056,c1057,c1058,c1059,c1060,c1061,c1062,c1063,c1064,c1065,c1066,c1067,c1068,c1069,c1070,c1071,c1072,c1073,c1074,c1075,c1076,c1077,c1078,c1079,c1080,c1081,c1082,c1083,c1084,c1085,c1086,c1087,c1088,c1089,c1090,c1091,c1092,c1093,c1094,c1095,c1096,c1097,c1098,c1099,c1100,c1101,c1102,c1103,c1104,c1105,c1106,c1107,c1108,c1109,c1110,c1111,c1112,c1113,c1114,c1115,c1116,c1117,c1118,c1119,c1120,c1121,c1122,c1123,c1124,c1125,c1126,c1127,c1128,c1129,c1130,c1131,c1132,c1133,c1134,c1135,c1136,c1137,c1138,c1139,c1140,c1141,c1142,c1143,c1144,c1145,c1146,c1147,c1148,c1149,c1150,c1151,c1152,c1153,c1154,c1155,c1156,c1157,c1158,c1159,c1160,c1161,c1162,c1163" );
                        properties.setProperty("columns.types", "int,string,string,string,string,string,string,string,string,string,string,string,string,string,string,tinyint,tinyint,string,string,string,string,string,bigint,string,string,string,string,string,string,string,string,string,string,string,int,int,int,string,string,string,double,double,string,string,int,string,string,string,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,int,tinyint,tinyint,string,tinyint,tinyint,int,string,int,int,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,string,int,double,string,int,double,string,int,double,string,int,double,tinyint,string,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,string,int,double,string,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,string,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,string,tinyint,tinyint,tinyint,tinyint,string,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,double,double,double,double,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,tinyint,double,double,double,string,string,string,string,string,string,tinyint,tinyint,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,string,double,string,string,string,double,double,double,double,tinyint,tinyint,tinyint,tinyint,int,int,string,double,string,string,tinyint,string,double,tinyint,string,int,double,string,string,int,int,string,string,int,string,string,double,double,int,string,tinyint,tinyint,string,string,string,int,string,double,string,double,int,tinyint,tinyint,string,double,double,double,double,int,double,string,string,string,string,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,double,int,string,string,tinyint,string,string,tinyint,string,string,tinyint,string,string,tinyint,string,string,tinyint,int,double,double,double,double,int,int,int,double,double,double,double,double,int,double,int,int,int,double,double,int,double,int,double,int,double,int,double,double,double,double,int,int,int,double,double,double,double,int,int,int,int,int,int,int,int,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,tinyint,string,tinyint,string,tinyint,tinyint,string,string,tinyint,string,tinyint,string,tinyint,string,tinyint,string,tinyint,string,tinyint,string,tinyint,string,tinyint,string,tinyint,string,tinyint,string,int,int,int,int,int,int,int,int,int,string,string,int,int,int,int,int,int,string,bigint,string,int,string,bigint,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,double,string,string,string,string,string,string,int,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,int,int,string,string,string,string,string,string,string,int,int,string,string,string,string,string,string,string,int,int,string,string,string,string,string,string,string,int,int,int,int,string,string,string,int,int,string,string,string,int,int,string,string,string,string,int,int,string,string,string,int,int,string,string,string,string,int,int,string,string,string,int,int,string,string,int,string,string,string,string,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,tinyint,int,string,string,string,string,string,int,int,int,int,int,string,double,string,string,double,string,string,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,string,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,string,string,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,string,string,string,string,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,string,string,string,string,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,double,double,double,double,double,double,double,double,double,double,double,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,double,double,double,double,double,double,double,double,double,double,double,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,string,string,string,string,double,double,double,double,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,double,double,double,double,double,double,double,double,double,double,double,double,double,double,double,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,double,int,int,int,string,string");


BytesWritable vblob = null;  // will contain value orc in byteswritable
BinarySortableSerDe vserde = null; // Will be used to converting value orc to byteswritabel


          SerDe serde = new OrcSerde();		// will be used to convert orcstruct to fields and get key value 
          
          try {
                                serde.initialize(new Configuration(), properties);
                        } catch (SerDeException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                        }

          Object fields=null;
                try {
                        fields = serde.deserialize(value);
                } catch (SerDeException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                }
          String keystr="";
          int keyIndex=1;
          String textRow = "";
          StructObjectInspector in=null;
                        try {
                                in = (StructObjectInspector) serde.getObjectInspector();
                                List <Object> vals = in.getStructFieldsDataAsList(fields);
                                
                                try { keystr = vals.get(keyIndex).toString(); }
                                catch(Exception ex1) {System.out.print("Catch1"); keystr="";}
                                String row="";
                				for (int k=0;k<vals.size();k++)
                				{
                					try {
                						row=row + vals.get(k).toString() + "|"; } catch(Exception e2) { row=row + "|"; }
                				}
                				row=row.substring(0,row.length() - 2);
                                
                                try {
                                	
                                    vblob = new BytesWritable(row.getBytes());
               				 } catch (Exception e) {
               					System.out.print("Catch2"); e.printStackTrace();
               				     textRow="";
               				 }  
                                
                        } catch (SerDeException e) {
                                // TODO Auto-generated catch block
                                keystr="";
                                e.printStackTrace();
                        }

                        if(keystr != "" && vblob!=null)
                        {

                        AccountNumID rKey = new AccountNumID(new Text(keystr),AccountNumID.ROSETTA_RECORD);
                        RosettaRecord rRecord = new RosettaRecord(keystr,vblob);
                        JoinGenericWritable rVal = new JoinGenericWritable(rRecord);
                        output.collect(rKey, rVal);
                        }
                        else
                        {
                        	System.out.println("vblo Null, " + keystr);
                        }
 

      }


  }
