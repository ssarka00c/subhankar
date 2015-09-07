package Monitor;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;
import org.json.simple.parser.ParseException;

public class MainMonitor {

	public static void main(String[] args) throws IOException, ParseException {
		// TODO Auto-generated method stub
		//String log4jConfPath = args[0].toString().trim() + "/log4j.properties";
		//PropertyConfigurator.configure(log4jConfPath);
		
		// Star Gate Monitor
		(new StartGate()).main(args);
		
		// Zookeeper Monitor
		(new zkMonitor()).main(args);
		
		// Kafka Broker Monitor
		(new kafkaMonitor()).main(args);
		
		// yarn Monitor
		(new rmMonitor()).main(args);
		
		//Hive Server 2 Monitor
		(new HS2Monitor()).main(args);

	}

}
