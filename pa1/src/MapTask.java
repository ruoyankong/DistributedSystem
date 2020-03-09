import java.util.List;


public class MapTask extends Task {
	String filename;
	String mid_filename;

	public MapTask(String filename, String mid_filename)
	{
		this.TaskType="Map";
		this.TaskOutput = "Null";
		this.filename = filename;
		this.mid_filename = mid_filename;
	}
}