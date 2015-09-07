package Monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.sql.*;

public class StartGate {
	
	List<String> chk = new ArrayList<String>();
	
	public StartGate()
	{}
	
	public static void main(String[] args)
	{
		StartGate sg = new StartGate();

		System.out.println("-------------------------------------------------  START OF HTTP HEADER CHECK -------------------------------------------------");
		
		if(args.length != 1)
		{
			System.out.println("Program Needs To Be Triggered With the Argument of DirLoc to Lookup");
			System.exit(1);
		}
		
		try
		{
			String path = args[0] + "/stargate.txt";
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
				String[] vals = line.split("=");
				boolean b = sg.checkUrlHeader(vals[1]);
				if(b==true)
				{
					sg.chk.add("replace into monitor.check_status set component_name='" + 
								vals[0] + "', status='OK' , lastchecked=now()");
					
					System.out.println(vals[0] + " -> OK" );
				}
				else
				{
					sg.chk.add("replace into monitor.check_status set component_name='" + 
							vals[0] + "', status='NOTOK' , lastchecked=now()");
					System.out.println(vals[0] + " -> NOTOK" );
				}
			}
			
			//Send List for MySQL Update
			System.out.println("Wait... Updating AlertDB");
			sg.updateMySQL(sg.chk);
			System.out.println("-------------------------------------------------  END OF HTTP HEADER CHECK -------------------------------------------------");
		
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			System.exit(1);
		}
		
	}

	
	public boolean checkUrlHeader(String url)
	{
		URL obj;
		try {
			obj = new URL(url);
		
		URLConnection conn = obj.openConnection();
		
		//get all headers
		Map<String, List<String>> map = conn.getHeaderFields();
		String hdr="";
		for (Map.Entry<String, List<String>> entry : map.entrySet()) {
			
			hdr = hdr + entry.getValue();
			
			// System.out.println("Key : " + entry.getKey() + 
	        //         " ,Value : " + entry.getValue());
		}
				
		if (hdr.contains("200 OK"))
			return true;
		else
			return false;
		
		} 	catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
			} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	public void updateMySQL(List<String> lst)
	{
		String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		String DB_URL = "jdbc:mysql://server-ch2-h001p/monitor";
		String user="monitor", pass="monitor";
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(DB_URL,user,pass);
			stmt = conn.createStatement();
			for (String str : lst)
			{
				stmt.execute(str);				
			}
			
			stmt.close();
			conn.close();
		}
			
		 catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
}
