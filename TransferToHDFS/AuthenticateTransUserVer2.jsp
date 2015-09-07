<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@ page import="java.sql.*" %>
<%@ page import="java.io.*" %>
<%@ page import="java.util.Scanner " %>
<%@ page import="java.util.UUID " %>


<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
</head>
<body> 


<%



String JDBC_DRIVER = "com.mysql.jdbc.Driver";
String DB_URL = "jdbc:mysql://mysql-server-hostname/trans";
String res="";
String user="",pass="",env="";
int i=0;

try {

if (request.getParameter("username") == null) {
    System.out.println("No User Found");
    i=1;
} else {
    user = request.getParameter("username");
}

if (request.getParameter("token") == null) {
    System.out.println("No Pass Found");
    i=1;
} else {
    pass = request.getParameter("token");
}

if (request.getParameter("env") == null) {
        env="prodcluster";
}
else{
        String p = request.getParameter("env");
        if(p.trim().equalsIgnoreCase("prod"))
                env="prodcluster";
        else
                {
                if(p.trim().equalsIgnoreCase("stage"))
                        env="stgcluster";
                else
                {
                        if(p.trim().equalsIgnoreCase("dev"))
                                env="devcluster";
                        else
                        {
                             
                        	if(p.trim().equalsIgnoreCase("dr"))
                        	{
                        		env="drcluster";
                        	}
                        	else
                        	{
                        	
                        	System.out.println("No Env Found");
                                i=1;
                        	}
                        }
                }
                }

}


//connect to mysql

if (i==0)
{
Connection conn = null;
Statement stmt = null;

Class.forName("com.mysql.jdbc.Driver");
conn = DriverManager.getConnection(DB_URL,"trans","trans");

stmt = conn.createStatement();
String sql;
sql = "SELECT cluster_name from trans.user_details where user_name='" + user.trim() + "' " +
      " and pass='" + pass.trim() + "' and status ='ENABLED' and cluster_name='" + env + "'";
ResultSet rs = stmt.executeQuery(sql);
//System.out.println("ok1");
        while(rs.next())
        {
                res=rs.getString("cluster_name").trim();
        }
        rs.close();
        stmt.close();
        conn.close();

        //System.out.println(res);
} //end of conn if


}  catch(Exception ex)
        { i=1; System.out.println(ex.toString()); }

if (res.trim().equalsIgnoreCase("prodcluster"))
{
        int fail=0;
        String cmdOut="";
        try
        {
        //success authentication find out the active namenode from zookeeper
        String fn="/tmp/" + UUID.randomUUID().toString().trim();
        //Process p = Runtime.getRuntime().exec("if [ -f /tmp/zkc.trans.subh ];then rm -f /tmp/zkc.trans.subh; fi");
        Process p = Runtime.getRuntime().exec(new String[] { "/bin/bash", "-c", "/usr/lib/zookeeper/bin/zkCli.sh -server server-ch2-d103p,server-ch2-d125p,server-ch2-d147p,server-ch2-d169p,server-ch2-d192p get /hadoop-ha/prodcluster/ActiveBreadCrumb 2>/dev/null|strings|tail -1 > " + fn } );
        p.waitFor();

        //Thread.sleep(3000);
        cmdOut = new Scanner(new File(fn)).useDelimiter("\\Z").next();

        Process p2 = Runtime.getRuntime().exec(new String[] { "/bin/bash", "-c", "rm -f " + fn } );
        p2.waitFor();

   

       if(cmdOut.trim().startsWith("server") && cmdOut.trim().endsWith("net"))
       {
        fail=0;
       }
       else
       {
        cmdOut="none";
       }

        } catch (Exception ex) { System.out.println(ex.toString()); fail=1;  }
        //System.out.println("ok:" + res);
         %>RES,success,<%= cmdOut %>

         <%
}               //END OF prodcluster
else
{
        if(res.trim().equalsIgnoreCase("stgcluster"))
        {
                %>RES,success,stage-namenode-hostname

        <%
        }                       //End OF  stage cluster
        else
        {
                if (res.trim().equalsIgnoreCase("devcluster"))
                {

                        %>RES,success,dev-namenode-hostname

                <%

                }               //End Of Dev Cluster
                else    //Means Not Valid Env Found
                {
                
                	if(res.trim().equalsIgnoreCase("drcluster"))
                	{
                		
                		int failDR=0;
                        String cmdOutDR="";
                        
                        try
                        {
                        //success authentication find out the active namenode from zookeeper
                        String fn="/tmp/" + UUID.randomUUID().toString().trim();
                        //Process p = Runtime.getRuntime().exec("if [ -f /tmp/zkc.trans.subh ];then rm -f /tmp/zkc.trans.subh; fi");
                        Process p = Runtime.getRuntime().exec(new String[] { "/bin/bash", "-c", "/usr/lib/zookeeper/bin/zkCli.sh -server server-po-c001p,server-po-c002p,server-po-c003p,server-po-c005p get /hadoop-ha/drcluster/ActiveBreadCrumb 2>/dev/null|strings|tail -1 > " + fn } );
                        p.waitFor();

                        //Thread.sleep(3000);
                        cmdOutDR = new Scanner(new File(fn)).useDelimiter("\\Z").next();

                        Process p2 = Runtime.getRuntime().exec(new String[] { "/bin/bash", "-c", "rm -f " + fn } );
                        p2.waitFor();

                       

                       if(cmdOutDR.trim().startsWith("server") && cmdOutDR.trim().endsWith("net"))
                       {
                        failDR=0;
                       }
                       else
                       {
                        cmdOutDR="none";
                       }

                        } catch (Exception ex) { System.out.println(ex.toString()); failDR=1;  }
                        //System.out.println("ok:" + res);
                         %>RES,success,<%= cmdOutDR %>

                         <%
                	}               //END OF drcluster
                	
                	else
                	{
                			System.out.println("F");
                		%>RES,fail
                		<%
                	}
                
                }
        }



}


%>
</body>
</html>

