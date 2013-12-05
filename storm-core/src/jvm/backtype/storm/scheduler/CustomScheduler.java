package backtype.storm.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.weakling.CheckWeaklings;

public class CustomScheduler implements IScheduler {

	private String topologyName;
	private Map<String,String> execSupMap; //executor to host mapping!
	//private String supervisor;
	//private String executor;
	
	private static final String SP = "spout";
	private static final String HOST = "hostname"; 

	@Override
	public void prepare(Map conf) {
	  //Take this from Nimbus conf!!
      //topologyName = (String) conf.get(Config.STORM_SCHEDULER1_TOPOLOGY_NAME);
      //execSupMap = (Map<String, String>) conf.get(Config.STORM_SCHEDULER1_TOPOLOGY_NAME);
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
	  	System.out.println("DemoScheduler: begin scheduling");
	  	
	  	if (null != cluster.getWeakHosts()) {
	  		System.out.println(cluster.getWeakHosts());
	  	}
	  
	  	//new EvenScheduler().schedule(topologies, cluster);
	  	
	  	Map<String,List<ExecutorDetails>> weakH = cluster.getWeakHosts();
	  	Set<String> keys = weakH.keySet();
	  	
	  	Collection<TopologyDetails> topos = topologies.getTopologies();
	  	
	  	for (String key : keys) {
	  		for (ExecutorDetails execDet : weakH.get(key)) {
	  			for (TopologyDetails topology : topos) {
	  				if (topology.getExecutors().contains(execDet)) {
	  					cluster.rmExecutorAssignment(topology.getId(), execDet);
	  				}
	  			}
	  		}
	  	}
	  	
	  	//now schedule evenly, weak slots will be omitted while assignments
	  	new EvenScheduler().schedule(topologies, cluster);
	  	
	  	for (String key : keys) {
	  		weakH.get(key).removeAll(weakH.get(key));
	  	}
	  	
	  	CheckWeaklings.getInstance().setWeakHosts(weakH);
        
	}

}
