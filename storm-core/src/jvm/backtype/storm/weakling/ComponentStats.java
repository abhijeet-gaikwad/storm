package backtype.storm.weakling;

import java.util.ArrayList;
import java.util.List;

public class ComponentStats {
	
  private String compId;
  private List<TaskStats> tStats = new ArrayList<TaskStats>();
  
  public String getCompId() {
	return compId;
  }
  
  public void setCompId(String compId) {
	this.compId = compId;
  }

  public List<TaskStats> gettStats() {
	return tStats;
  }

  public void settStats(List<TaskStats> tStats) {
	this.tStats = tStats;
  }
  
  public void addTotStats(TaskStats tStat) {
	this.tStats.add(tStat);
  }
  
  @Override
	public String toString() {
		// TODO Auto-generated method stub
		return "ComponentStats[compId:" + compId + " tStats:" + tStats + "]";
	}
}
