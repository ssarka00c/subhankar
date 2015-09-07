package Monitor;

import org.json.simple.JSONObject;

public class rmQueue {
	
	public String queueName="";
	public long  memory=0, vcore=0, containerUsed=0, pendingApps=0,allocatedMB=0, totalMB=0,activeNodes=0,lostNodes=0,unhealthyNodes=0,totalVcores=0;
	public double absoluteUsedCapacity=0;
	public int numApplications=0, numActiveApplications=0;
	public JSONObject users=null;
	
	public rmQueue()
	{}

}
