package backtype.storm.weakling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.thrift7.TException;

import backtype.storm.generated.BoltStats;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.TopologySummary;
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
	private Set<String> weakHosts = new HashSet<String>();
	private boolean flag = false;

	public Set<String> getWeakHosts() {
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

		findOutliers(tStats); // TODO stub!

		weakHosts.add("localhost");
		if (flag) {
			weakHosts.add("abc");
		} else {
			flag = true;
		}

		// for each topology find weaklings

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
						cS.setHost(eSummary.get_host());
					}

					TaskStats tskS = new TaskStats();
					tskS.setExecInfo(eSummary.get_executor_info());
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

	private void findOutliers(List<TopoStats> tStats) {
		// TODO Auto-generated method stub

	}

}
