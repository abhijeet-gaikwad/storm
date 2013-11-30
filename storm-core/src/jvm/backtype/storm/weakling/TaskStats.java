package backtype.storm.weakling;

import backtype.storm.generated.ExecutorInfo;
import backtype.storm.scheduler.ExecutorDetails;

public class TaskStats {
	
	private ExecutorDetails execInfo;
	int executors;
	double execLatencies;
	
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
		return "TaskStats[execInfo:" + execInfo + " execLatencies:" + execLatencies + "]";
	}
}
