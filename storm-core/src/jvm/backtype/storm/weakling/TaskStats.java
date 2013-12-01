package backtype.storm.weakling;

import backtype.storm.generated.ExecutorInfo;
import backtype.storm.scheduler.ExecutorDetails;

public class TaskStats implements Comparable<TaskStats>{
	
	private ExecutorDetails execInfo;
	int executors; //may be used in future!
	double execLatencies;
	private String host;
	
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public ExecutorDetails getExecInfo() {
		return execInfo;
	}
		
	public void setExecInfo(ExecutorInfo execInfo) {
		this.execInfo = new ExecutorDetails(execInfo.get_task_start(), execInfo.get_task_end());
		
	}
	
	public void setExecdetails(ExecutorDetails execInfo) {
		this.execInfo = new ExecutorDetails(execInfo.getStartTask(), execInfo.getEndTask());
	}
	
	public int getExecutors() {
		return executors;
	}
	
	public void setExecutors(int executors) {
		this.executors = executors;
	}
	
	public double getExecLatencies() {
		return execLatencies;
	}
	
	public void setExecLatencies(double execLatencies) {
		this.execLatencies = execLatencies;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "TaskStats[execInfo:" + execInfo + " Host:" + host + " execLatencies:" + execLatencies + "]";
	}

	@Override
	public int compareTo(TaskStats o) {
		if (this.execLatencies > o.execLatencies) {
			return 1;
		} else if (this.execLatencies < o.execLatencies) {
			return -1;
		}
		return 0;	
	}
}
