public abstract class Task {
	String TaskType;
	String TaskOutput;
	long TaskDuration;
	Address TaskNodeInfo;
	int LoadProb;

	public void FillTaskOutput(String _val)
	{
		this.TaskOutput = _val;
	}
}