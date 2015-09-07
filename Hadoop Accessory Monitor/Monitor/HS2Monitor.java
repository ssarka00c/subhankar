package Monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class HS2Monitor {

	static List<String> chk = new ArrayList<String>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	System.out.println("-------------------------------------------------  START OF Beeline CHECK -------------------------------------------------");
		
		if(args.length != 1)
		{
			System.out.println("Program Needs To Be Triggered With the Argument of DirLoc to Lookup");
			System.exit(1);
		}
		
		try
		{
		String path = args[0] + "/beeline.txt";
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
			String host="";
			int port = 10000;
			if(vals[1].contains(":"))
			{
				String[] hpa = vals[1].split(":");
				host = hpa[0].trim();
				try { port = Integer.parseInt(hpa[1].trim()); } catch(Exception ex1){}
				if(checkBeeline(host,port))
				{
					chk.add("replace into monitor.check_status set component_name='" + 
							vals[0] + "-" + host + ":" + port + "', status='OK' , lastchecked=now()");
					System.out.println(vals[0] + " - " + host + " ->  OK");
				}
				else
				{
					chk.add("replace into monitor.check_status set component_name='" + 
							vals[0] + "-" + host + ":" + port + "', status='NOTOK' , lastchecked=now()");
					System.out.println(vals[0] + " - " + host + " ->  NOTOK");
				}
			}
			
			
		}
		
		} catch(Exception ex)
		{ex.printStackTrace(); System.exit(1);}
		
		System.out.println("Wait... Updating AlertDB");
		StartGate sg = new StartGate();
		sg.updateMySQL(chk);
		
		System.out.println("-------------------------------------------------  END OF Beeline CHECK -------------------------------------------------");
	}
	
	
	public static boolean checkBeeline(String host, int port)
	{
		try 
		{
			// JDBC driver name and database URL
			   final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";  
			   final String DB_URL = "jdbc:hive2://" + host + ":" + port + "/default";
			   
			//  Database credentials
			   final String USER = "hive";
			   final String PASS = "mypass";
			   
			   Connection conn = null;
			   Statement stmt = null;
			   
			   	  //STEP 2: Register JDBC driver
			      Class.forName(JDBC_DRIVER);

			      //STEP 3: Open a connection
			      //System.out.println("Connecting to Hive Server 2..." + host);
			      DriverManager.setLoginTimeout(30);
			      conn = DriverManager.getConnection(DB_URL,USER,PASS);
			      stmt = conn.createStatement();
			      String sql;
			      sql = "show tables";
			      ResultSet rs = stmt.executeQuery(sql);
			      
			     if(rs.next())
			     {
			    	 rs.close();
			    	 stmt.close();
			    	 conn.close();
			    	 return true;
			     }
			     else
			     {
			    	 rs.close();
			    	 stmt.close();
			    	 conn.close();
			    	 return false;
			     }
			      
			      
		
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
			return false;
		}
	}

}
