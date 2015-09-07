package Monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import org.I0Itec.zkclient.*;
import org.apache.zookeeper.ZooKeeper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class kafkaMonitor {
	
	public static List<String> db = new ArrayList();
	
	public static void main(String[] args)
	{
		if(db.size() > 0)
			db.clear();
	
		System.out.println("-------------------------------------------------  START OF KAFKA BROKER CHECK -------------------------------------------------");
		
		if(args.length != 1)
		{
			System.out.println("Program Needs To Be Triggered With the Argument of DirLoc to Lookup");
			System.exit(1);
		}
		
		try
		{
			String path = args[0] + "/broker.txt";
			File file = new File(path);
			if(!file.canRead())
			{
				System.out.println("Could Not Find " + path);
				System.exit(1);
			}
			
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line=null;
			while((line=br.readLine()) != null)
			{
				String[] vals = line.trim().split("=");
				
				if(vals.length != 2)
				{
					System.out.println("Invalid Lookup Entry : " + line);
					break;
				}
				
				if(vals[1].contains("|"))
				{
					//have valid split |
					String[] zkBr=vals[1].trim().split("\\|");
					if(zkBr.length != 3)
					{
						System.out.println("Invalid Entry : " + line);
					}
					String zkString = zkBr[0].trim();
					String[] brokers = zkBr[1].trim().split(",");
					String zPath=zkBr[2];
					
					if(!chkBroker(vals[0],zkString,brokers,zPath))
					{
						System.out.println("Failed");
					}
					
				}
			}
			
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		
		
		
		System.out.println("-------------------------------------------------  END OF KAFKA BROKER CHECK -------------------------------------------------");
	}
	
	
	public static boolean chkBroker(String componentName,String zkStr, String[] brkrs,String path)
	{
		
		try
		{
			Hashtable ht = new Hashtable();
			
			for(int i=0;i<brkrs.length;i++)
				ht.put(brkrs[i], "NOTOK");
			// System.out.println(zkStr);
			
			
			ZooKeeper zk = new ZooKeeper(zkStr, 10000, null);
			List<String> Ids = zk.getChildren(path,false);
			for(String ids:Ids)
			{
				String zData = new String(zk.getData(path + "/" + ids.trim(),false,null));
				// System.out.println("zData=" + zData);
				JSONParser jsonParser = new JSONParser();
				JSONObject jObj = (JSONObject) jsonParser.parse(zData);
				String host = jObj.get("host").toString();
				String port = jObj.get("port").toString();
			
				if(ht.containsKey(host))
					ht.put(host, "OK");
				
			}
			

			
			Enumeration enumeration = ht.keys();
			while(enumeration.hasMoreElements())
			{
				String key = enumeration.nextElement().toString();
				String val = ht.get(key).toString();
				db.add("replace into monitor.check_status set component_name='" + 
						componentName + "-"  + key + "', status='" + val + "' , lastchecked=now()");
				System.out.println(componentName + "-" + key + " -> " + val);		
			}
			
			System.out.println("Wait... Updating AlertDB");
			StartGate sg = new StartGate();
			sg.updateMySQL(db);
			return true;
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return false;
		}
		
	}

}
