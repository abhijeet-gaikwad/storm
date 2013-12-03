package backtype.storm.weakling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift7.TException;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.utils.NimbusClient;

/**
 * 
 * @author abhijeet Finds weak hosts.
 * 
 */
public class CheckWeaklings {

	private Map conf;
	private Client tClient;
	private static CheckWeaklings instance = null;
	private Map<String,List<ExecutorDetails>> weakHosts = new HashMap<String,List<ExecutorDetails>>();
	private boolean flag = false;

	public Map<String, List<ExecutorDetails>> getWeakHosts() {
		return weakHosts;
	}

	private CheckWeaklings(Map config) {
		conf = new HashMap(config);
		tClient = NimbusClient.getConfiguredClient(conf).getClient();
	}

	public static synchronized CheckWeaklings getInstance(Map config) {
		if (null == instance) {
			instance = new CheckWeaklings(config);
		}
		return instance;
	}

	public static synchronized CheckWeaklings getInstance() {
		if (null == instance) {
			System.out
					.println("Instance not created yet, call getInstance(map) first.");
		}
		return instance;
	}

	/**
	 * This method returns the list of blacklisted hosts
	 * 
	 * @return set of blacklisted hosts!
	 */
	public void chkWeaklings() {

		List<TopoStats> tStats = getTopologyStats();
		System.out.println(tStats);

		findWeakHosts(findOutliers(tStats), tStats);

		/*weakHosts.add("localhost");
		if (flag) {
			weakHosts.add("abc");
		} else {
			flag = true;
		}*/

		// for each topology find weaklings

	}

	private void findWeakHosts(List<TopoStats> modi, List<TopoStats> orig) {
		Map<String,List<ExecutorDetails>> naya, purana;
		
		naya = getHostMappings(modi);
		purana = getHostMappings(orig);
		
		Set<String> keys = naya.keySet();
		for (String host : keys) {
			if (naya.get(host).size() / purana.get(host).size() > .5) {
				weakHosts.put(host, naya.get(host));
			}
		}
	}

	private Map<String,List<ExecutorDetails>> getHostMappings(List<TopoStats> st) {
		Map<String, List<ExecutorDetails>> mp = new HashMap<String, List<ExecutorDetails>>();
		
		for (TopoStats tS : st) {
			for (ComponentStats cS : tS.getcStats()) {
				for (TaskStats tskS : cS.gettStats()) {
					String host = tskS.getHost();
					
					if (mp.containsKey(host)) {
						mp.get(host).add(tskS.getExecInfo());
					} else {
						List<ExecutorDetails> eD = new ArrayList<ExecutorDetails>();
						eD.add(tskS.getExecInfo());
						mp.put(host, eD);
					}
				}
			}
		}
		
		return mp;
	}
	
	private List<TopoStats> getTopologyStats() {
		ClusterSummary sum;
		List<TopoStats> tStats = new ArrayList<TopoStats>();

		try {
			sum = tClient.getClusterInfo();

			//System.out.println(sum.toString());

			List<TopologySummary> listTopos = sum.get_topologies();

			for (TopologySummary summ : listTopos) {
				TopologyInfo info = tClient.getTopologyInfo(summ.get_id());
				TopoStats tS = new TopoStats();
				tS.setId(info.get_id());
				tS.setName(info.get_name());
				List<ExecutorSummary> eSummaries = info.get_executors();
				for (ExecutorSummary eSummary : eSummaries) {
					ComponentStats cS = null;
					
					if (eSummary.get_component_id().equals("__acker")) {
						continue; //leave ackers out!!
					}
					
					for (ComponentStats tmp : tS.getcStats()) {
						if (tmp.getCompId() == eSummary.get_component_id()) {
							cS = tmp;
						}
					}

					if (null == cS) {
						cS = new ComponentStats();
						cS.setCompId(eSummary.get_component_id());
					}

					TaskStats tskS = new TaskStats();
					tskS.setExecInfo(eSummary.get_executor_info());
					tskS.setHost(eSummary.get_host());
					if (eSummary.get_stats().get_specific().is_set_bolt()) {
						double avg = 0;
						Map<GlobalStreamId, Double> tmp = eSummary.get_stats().get_specific()
								.get_bolt().get_execute_ms_avg()
								.get(":all-time"); // all-time

						//System.out.println(tmp.size() + " " + tmp);
						for (GlobalStreamId gSI : tmp.keySet()) {
							//System.out.println(tmp.get(gSI));
							avg = avg + tmp.get(gSI); // stream id
						}

						tskS.setExecLatencies(avg / tmp.size());
					} else {
						// spout encountered, skip!
						continue;
					}
					cS.addTotStats(tskS);
					tS.addTocStats(cS);
				}
				tStats.add(tS);
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return tStats;
	}

	private List<TopoStats> findOutliers(List<TopoStats> tStats) {
		List<TopoStats> tpSts = tStats;
		
		for (TopoStats tS : tpSts) {
			for (ComponentStats cS  : tS.getcStats()) {
				findOutliersPerTasks(cS.gettStats());
			}
		}

		return tpSts;
	}
	
	private void findOutliersPerTasks(List<TaskStats> list) {
		
		Collections.sort(list);
		
		int n = list.size();
		int qIdx = (n+1)/4;	////lower quartile
		double q1 = list.get(qIdx - 1).execLatencies;
		if (0 != (n+1) % 4) {
			q1 = (list.get(qIdx - 1).execLatencies + list.get(qIdx).execLatencies)/2;
		}
		
		qIdx = (3*(n+1))/4;	//upper quartile
	    double q2 = list.get(qIdx - 1).execLatencies;
	    if (0 != (3*(n+1)) % 4) {
			q2 = (list.get(qIdx - 1).execLatencies + list.get(qIdx).execLatencies)/2;
		}
	    
	    double IQR = q2 - q1;
	    double upFence = q2 + IQR*1.5;
	    
	    for (int i = 0; i < qIdx; i++) {
	    	list.remove(i);
	    }
	    
	    for (int i = 0; i < list.size(); ++i) {
	    	if (list.get(i).execLatencies < upFence) {
	    		list.remove(i);
	    	} else {break;}
	    }
	}

}
