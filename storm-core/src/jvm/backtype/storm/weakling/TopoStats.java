package backtype.storm.weakling;

import java.util.ArrayList;
import java.util.List;

public class TopoStats {

	private String id;
	private String name;
	private List<ComponentStats> cStats = new ArrayList<ComponentStats>();
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public List<ComponentStats> getcStats() {
		return cStats;
	}
		
	public void setcStats(List<ComponentStats> cStats) {
		this.cStats = cStats;
	}
	
	public void addTocStats(ComponentStats cStats) {
		this.cStats.add(cStats);
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "TopoStats[id:" + id + " name:" + name + " cStats:" + cStats + "]";
	}
}
