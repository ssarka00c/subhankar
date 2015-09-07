package Monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.I0Itec.zkclient.*;


public class zkMonitor {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		System.out.println("-------------------------------------------------  START OF ZOOKEEPER CHECK -------------------------------------------------");
		
		if(args.length != 1)
		{
			System.out.println("Program Needs To Be Triggered With the Argument of DirLoc to Lookup");
			System.exit(1);
		}
		
		try
		{
		String path = args[0] + "/zookeeper.txt";
		File file = new File(path);
		if(!file.canRead())
		{
			System.out.println("Could Not Find " + path);
			System.exit(1);
		}
		PortListening pl = new PortListening();
		
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String line=null;
		while((line=br.readLine()) != null)
		{
			String[] vals = line.split("=");
			String host="";
			int port = 2181;
			if(vals[1].contains(","))
			{
				String[] hpa = vals[1].split(",");
				for(String str : hpa)
				{
					if(zkMonitor(str))
					{
						pl.chk.add("replace into monitor.check_status set component_name='" + 
								vals[0] + "-" + str + "', status='OK' , lastchecked=now()");
					}
					else
					{
						pl.chk.add("replace into monitor.check_status set component_name='" + 
								vals[0] + "-" + str + "', status='NOTOK' , lastchecked=now()");
					}
				}
			}
			else
			{
				if(vals[1].contains(":"))
				{
					
					if(zkMonitor(vals[1]))
					{
						pl.chk.add("replace into monitor.check_status set component_name='" + 
								vals[0] + "-" + vals[1] + "', status='OK' , lastchecked=now()");
					}
					else
					{
						pl.chk.add("replace into monitor.check_status set component_name='" + 
								vals[0] + "-"  + vals[1] + "', status='NOTOK' , lastchecked=now()");
					}
				}
				else
					host = vals[1];
					if(zkMonitor(vals[1] + ":2181"))
					{
						pl.chk.add("replace into monitor.check_status set component_name='" + 
								vals[0] + "-"  + vals[1] + "', status='OK' , lastchecked=now()");
					}
					else
					{
						pl.chk.add("replace into monitor.check_status set component_name='" + 
								"ZK-" + vals[1] + "', status='NOTOK' , lastchecked=now()");
					}
					
			}
			
		}
		
		System.out.println("Wait... Updating AlertDB");
		StartGate sg = new StartGate();
		sg.updateMySQL(pl.chk);
		
		
		}
		catch(Exception ex) {ex.printStackTrace();}
		System.out.println("-------------------------------------------------  END OF ZOOKEEPER CHECK -------------------------------------------------");
	
		
	}
	
	public static boolean zkMonitor(String zkAddress)
	{

		try
		{
		ZkClient zkc = new ZkClient(zkAddress,5000,5000);
		int n = zkc.countChildren("/");
		zkc.close();
		if(n > 0)
		{
			System.out.println(zkAddress + " -> OK" );
			return true;
		}
		else
		{
			System.out.println(zkAddress + " -> NOTOK" );
			return false;
		}
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			System.out.println(zkAddress + " -> NOTOK" );
			return false;
		}
	}

}
