package Monitor;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;


public class rmMonitor {

	public static List<rmQueue> rmqueues = new ArrayList();
	static Hashtable HTQueue = new Hashtable();
	static Hashtable HTQueueUser = new Hashtable();
	static String counterSql="";
	
	public static void main(String[] args) throws IOException, ParseException {
		// TODO Auto-generated method stub

		long allocatedMB=0, totalMB=0,activeNodes=0,lostNodes=0,unhealthyNodes=0,totalVcores=0;
		int allocatedMBPercent=0;
		
		System.out.println("-------------------------------------------------  START OF RM CHECK -------------------------------------------------");
		
		if(args.length != 1)
		{
			System.out.println("Program Needs To Be Triggered With the Argument of DirLoc to Lookup");
			System.exit(1);
		}
		
		String rmUri="http://rm-server-ch2-d145p:8088/ws/v1/cluster/scheduler";
		String filePath=args[0] + "/scheduler.json";
		
		//get the json response from yarn and save to file
		if(!saveYarnJsonResponse(filePath,rmUri))
		{
			System.out.println("Yarn Response Could Not Be Saved");
			System.exit(1);
		}
		
		
		//Parse another Json to get cluster metrics
		
		try
		{
			
			String rmUri2="http://rm-server-ch2-d145p:8088/ws/v1/cluster/metrics";
			String filePath2=args[0] + "/metrics.json";
			
			//get the json response from yarn and save to file
			if(!saveYarnJsonResponse(filePath2,rmUri2))
			{
				System.out.println("Yarn Response Could Not Be Saved 2");
				System.exit(1);
			}
			
		FileReader jsonReaderMetrics = new FileReader(new File(filePath2));
		JSONParser jsonParserMetrics = new JSONParser();
		JSONObject jObjMetrics = (JSONObject) jsonParserMetrics.parse(jsonReaderMetrics);
		JSONObject jobMetricDetails = (JSONObject) jObjMetrics.get("clusterMetrics");
		//availableMB=0, totalMB=0,activeNodes=0,lostNodes=0,unhealthyNodes=0,totalVcores=0;
		try { allocatedMB = (long) jobMetricDetails.get("allocatedMB"); } catch(Exception ex1) {allocatedMB=0;}
		try { totalMB = (long) jobMetricDetails.get("totalMB"); } catch(Exception ex1) {activeNodes=0;}
		try { activeNodes = (long) jobMetricDetails.get("activeNodes"); } catch(Exception ex1) {activeNodes=0;}
		try { lostNodes = (long) jobMetricDetails.get("lostNodes");  } catch(Exception ex1) {lostNodes=0;}
		try { unhealthyNodes = (long) jobMetricDetails.get("unhealthyNodes");  } catch(Exception ex1) {unhealthyNodes=0;}
		try { long l = (allocatedMB*100)/(totalMB);
		allocatedMBPercent = new Long(l).intValue();
		 } catch(Exception ex1) {allocatedMBPercent=0;} 
		//System.out.println("L:" + l);
		totalVcores = activeNodes * 24;
		
		
		
		// System.out.println("allocatedMB : " + allocatedMB + ", totalMB : " + totalMB + ",allocatedMBPercent : " + allocatedMBPercent);
		counterSql = "insert into COUNTER (DT,ALLOCATED_MB_PERCENT,TOTAL_VCORES,LOST_NODES,UNHEALTHY_NODES,TOTAL_MB,ACTIVE_NODES) "
				+ " VALUES(now()," + allocatedMBPercent + "," + totalVcores + "," + lostNodes + "," + unhealthyNodes + "," + totalMB + "," + activeNodes + ")";
		//System.out.println(counterSql);
		
		
		}
		catch(Exception exjf2)
		{
		
			exjf2.printStackTrace();
			System.out.println("Error Handling Metrics JSON");
			System.exit(1);
		}
		
		
		try
		{
		// Browse through json response 1 and find leaf queues
			
		FileReader jsonReader = new FileReader(new File(filePath));
		JSONParser jsonParser = new JSONParser();
		JSONObject jObjL1 = (JSONObject) jsonParser.parse(jsonReader);
		
		JSONObject jObjL2 = (JSONObject) jObjL1.get("scheduler");
		JSONObject jObjL3 = (JSONObject) jObjL2.get("schedulerInfo");
		String topQueue = jObjL3.get("queueName").toString();
		
		if(!topQueue.equalsIgnoreCase("root"))
		{
			System.out.println("root queue not detected...");
			System.exit(1);
		}
		
		JSONObject allQueues = (JSONObject) jObjL3.get("queues");
		JSONArray queues = (JSONArray) allQueues.get("queue");
		
		for(int q=0; q<queues.size();q++)
		{
			JSONObject qdetail = (JSONObject) queues.get(q);
			if(!addToListNextQueuesInfo(qdetail))
			{
				System.out.println("Unable To Get Leaf Queue" + q);
				System.exit(1);
			}
			
		}
		
		} catch(Exception ex) {ex.printStackTrace();}
		

		//Call the update db routine 
		if(!updateDb())
		{
			System.out.println("Unable To Update MySql");
		}
		
		System.out.println("-------------------------------------------------  END OF RM CHECK -------------------------------------------------");
			
	}
	
	
	//find all leaf queue
	public static boolean addToListNextQueuesInfo(JSONObject jobj)
	{
		boolean isLeaf=false;
		try
		{
			String leafType="";
			try
			{
				leafType = jobj.get("type").toString().trim();
				if(leafType.contains("capacitySchedulerLeafQueueInfo"))
					isLeaf=true;
				
			} catch(Exception ex1) { isLeaf=false;}
			//System.out.println(jobj.get("queueName").toString());
			if(isLeaf == false)
			{
				
				JSONObject allQueues = (JSONObject) jobj.get("queues");
				JSONArray queues = (JSONArray) allQueues.get("queue");
				for(int q=0; q<queues.size();q++)
				{
					JSONObject qdetail = (JSONObject) queues.get(q);
					addToListNextQueuesInfo(qdetail);
				}
			}
			else
			{
				rmQueue rmq = new rmQueue();
				rmq.queueName = jobj.get("queueName").toString();
				try { rmq.absoluteUsedCapacity = (double) jobj.get("absoluteUsedCapacity");} catch(Exception ex1) {rmq.absoluteUsedCapacity=0;}
				try{rmq.numApplications=Integer.parseInt(jobj.get("numApplications").toString());} catch(Exception ex1) {rmq.numApplications=0;}
				try{rmq.numActiveApplications = Integer.parseInt(jobj.get("numActiveApplications").toString());} catch(Exception ex1) {rmq.numActiveApplications=0;}
				try{rmq.containerUsed = Integer.parseInt(jobj.get("numContainers").toString());} catch(Exception ex1) {rmq.containerUsed=0;}
				try{rmq.pendingApps = Integer.parseInt(jobj.get("numPendingApplications").toString());} catch(Exception ex1) {rmq.pendingApps=0;}
				try{JSONObject jbj = (JSONObject) jobj.get("resourcesUsed");
				rmq.memory =  (long) jbj.get("memory");
				rmq.vcore = (long) jbj.get("vCores");
				} catch (Exception ex1){rmq.memory=0;rmq.vcore=0;}
				
				//any negative value , mark as 0
				if(rmq.absoluteUsedCapacity < 0)
					rmq.absoluteUsedCapacity=0;
				if(rmq.numApplications < 0)
					rmq.numApplications=0;
				if(rmq.numActiveApplications<0)
					rmq.numActiveApplications=0;
				if(rmq.containerUsed<0)
					rmq.containerUsed=0;
				if(rmq.pendingApps<0)
					rmq.pendingApps=0;
				
				
				try { rmq.users = (JSONObject)jobj.get("users"); } catch(Exception e2) {rmq.users=null;}
				
				System.out.println("Queue=" + rmq.queueName + ",Memory=" + rmq.memory + ",vCores=" + rmq.vcore);
				rmqueues.add(rmq);
				String qSql = "INSERT INTO RM_USAGE_BY_QUEUE (RUNID,QUEUE_NAME,QUEUE_AB_USER_CAP,QUEUE_APPS_RUNNING,QUEUE_MEM_USAGE,QUEUE_VCORE_USAGE,QUEUE_CONTAINER_USAGE,QUEUE_PENDING_APPS) " + 
						  " VALUES($$RUNID,'" + rmq.queueName + "'," +  rmq.absoluteUsedCapacity + "," + rmq.numActiveApplications + "," +  rmq.memory + "," + rmq.vcore + "," + rmq.containerUsed + "," + rmq.pendingApps + ")";
		
				if(!updateHashtable(HTQueue,rmq.queueName,qSql,false)) { System.out.println("Unable to Update HashTable, Abort"); System.exit(1); }
		
				//find the queue-user details
				if(rmq.users != null)
				{
					if(!populateQueueUser(rmq))
					{
						System.out.println("Unable To Populate User Details");
						System.exit(1);
					}
				}
				
			}
			
			return true;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return false;
		}
	}
	
	
	//find all queue-user info
	public static boolean populateQueueUser(rmQueue rmq)
	{
		try
		{
			//JSONObject users = (JSONObject) qdetail.get("users");
			JSONArray userDetail = (JSONArray) rmq.users.get("user");
			for (int u=0;u<userDetail.size();u++)
			{					
				JSONObject user = (JSONObject) userDetail.get(u);
				String userName= user.get("username").toString();
				long userPendingApps = (long) user.get("numPendingApplications");
				long userRunningApps = (long) user.get("numActiveApplications");
				JSONObject userMemVcore = (JSONObject) user.get("resourcesUsed");
				long userMem = (long) userMemVcore.get("memory");
				long userVcore = (long) userMemVcore.get("vCores");
				
				
				String qUserSql = "INSERT INTO RM_USAGE_BY_QUEUE_USER (RUNID,QUEUE_NAME,USER_NAME,USER_MEM_USAGE,USER_VCORE_USAGE,USER_RUNNING_APPS,USER_PENDING_APPS) " +
								  " VALUES($$RUNID,'" + rmq.queueName + "','" + userName + "'," + userMem + "," + userVcore + "," + userRunningApps + "," + userPendingApps  + ")";
				//System.out.println(qUserSql);
				
				if(!updateHashtable(HTQueueUser,rmq.queueName + "-" + userName,qUserSql,false)) 
				{ 
					System.out.println("Unable to Update HashTable, Abort"); 
					System.exit(1); 
				}
				
				//System.out.println(qUserSql);
				
			}
			
			return true;
		}
		catch(Exception ex){return false;}
	}
	
	//Get yarn response and save in file
	public static boolean saveYarnJsonResponse(String filePath,String uri)
	{
		try
		{
			 URL url = new URL(uri);
			 String jsonResponse = org.apache.commons.io.IOUtils.toString(url.openStream());
			 File jsonFile = new File(filePath);
			 FileUtils.writeStringToFile(jsonFile, jsonResponse);
			 if ((new File(filePath).exists()))
					 return true;
			 else
				 return false;
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return false;
		}
	}
	
	
	@SuppressWarnings("rawtypes")
	private static boolean updateHashtable(Hashtable ht,String key, String val, boolean updt)
	{
		try
		{
			if(ht.containsKey(key))
			{
				if(updt==true)
				{
					double tval = ((Double) ht.get(key)).doubleValue() ;
					double rcvd = Double.valueOf(val).doubleValue();
					double newVal = tval + rcvd;
					ht.put(key, newVal);
					return true;
				}
				else
				{
					ht.put(key, val);
					return true;
				}
			}	//End Of IF the Key was already present
			else
			{
				ht.put(key, val);
				return true;
			}
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return false;
		}
	}


	public static boolean updateDb()
	{
		Enumeration qKeys,qUserKeys;
		try
		{
			String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
			String DB_URL = "jdbc:mysql://server-ch2-h001p/infra";
			String user="infra", pass="infra";
			Connection conn = null;
			Statement stmt = null;
			ResultSet rs;
			
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DB_URL,user,pass);
			
			//Step 1 - Get max runid from COUNTER
			String qry = "select max(RUNID) RUNID from infra.COUNTER";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(qry);
			if(!rs.next())
			{
				System.out.println("Could Not Fetch RUN ID FROM MYSQL DB H001P");
				rs.close();
				stmt.close();
				conn.close();
				System.exit(1);
			}
			
			long runid = rs.getLong("RUNID");
			runid++;
			
			String sql = "delete from RM_USAGE_BY_QUEUE where RUNID=" + runid;
			stmt.execute(sql);
			sql = "delete from RM_USAGE_BY_QUEUE_USER where RUNID=" + runid;
			stmt.execute(sql);
			
			
			//System.out.println("Current Run ID : " + runid);
			
			qKeys = HTQueue.keys();
			while(qKeys.hasMoreElements())
			{
				sql = HTQueue.get(qKeys.nextElement()).toString().replaceAll("\\$\\$RUNID", String.valueOf(runid));
				// System.out.println(sql);
				stmt.execute(sql);
				//System.out.print("OK");
				
				
			} // END OF WHILE FOR QUEUES HT
			
			
			qUserKeys = HTQueueUser.keys();
			while(qUserKeys.hasMoreElements())
			{
				
				sql = HTQueueUser.get(qUserKeys.nextElement()).toString().replaceFirst("\\$\\$RUNID", String.valueOf(runid));
				//System.out.println(sql);
				stmt.execute(sql);
				//System.out.print("OK");
				
			} // END OF WHILE FOR QUEUE USERS HT
			
			long delId = 0;
			if(runid > 7200)
				delId=runid-3600;
			
			//delId = runid - delId;
			
			//If All Transactions were successfull increment the COUNTER BY ONE;
			//System.out.println(counterSql);
			stmt.execute(counterSql);
			//System.out.println("Counter Incremented.. All Done");
			
			//Purge Old Values
			sql = "delete from RM_USAGE_BY_QUEUE where RUNID < " + delId;
			//System.out.println(sql);
			stmt.execute(sql);
			//System.out.print("OK");
			sql = "delete from RM_USAGE_BY_QUEUE_USER where RUNID < " + delId;
			//System.out.println(sql);
			stmt.execute(sql);
			//System.out.print("OK");
			
			//close mysql connection
			rs.close();
			stmt.close();
			conn.close();
			System.out.println("ALL OK");
			return true;
		} //END OF MYSQL try block
		catch(Exception exm)
		{
			exm.printStackTrace();
			System.out.println("Exception in MYSQL Block");
			return false;
		}
	}
	
}
