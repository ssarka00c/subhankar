package Monitor;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class PortListening {
	
	DataOutputStream dout ;
	DataInputStream din;
	
	List<String> chk = new ArrayList<String>();
	
	public void PortListening() {}

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
					String[] hps = str.split(":");
					host=hps[0];
					port = Integer.parseInt(hps[1].trim());
					if(pl.serverListening(pl, host, port))
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
					String[] hps = vals[1].split(":");
					host=hps[0];
					port = Integer.parseInt(hps[1].trim());
					if(pl.serverListening(pl, host, port))
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
					if(pl.serverListening(pl, host, port))
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
	
	public  boolean serverListening(PortListening pl,String host, int port)
	{
	    Socket s = null;
	    try
	    {
	    	System.out.println(host + ":" + port);
	        s = new Socket(host, port);	     
	        s.setReceiveBufferSize(536870912);
	        s.setTcpNoDelay(true);
	        s.setKeepAlive(true);
	        s.setSoTimeout(15000);
	        
	        InputStreamReader inputstreamreader = new InputStreamReader(s.getInputStream());
	        BufferedReader bufferedreader = new BufferedReader(inputstreamreader);
	        PrintWriter printwriter = new PrintWriter(s.getOutputStream(),true);
	        printwriter.println("stat");
	        //Thread.sleep(5000);
	        String lineread = "";
	        while ((lineread = bufferedreader.readLine()) != null){
	         if(lineread.trim().startsWith("Mode:"))
	        	  System.out.println(host + ":" + port + " -> " + lineread);
	        }
	        
	        s.close();
	        
	        return true;
	    }
	    catch (Exception e)
	    {
	    	e.printStackTrace();
	        return false;
	    }
	    finally
	    {
	        if(s != null)
	            try {s.close();}
	            catch(Exception e){}
	    }
	}

}
