import java.util.List;


public class SortTask extends Task {
	List<String> mid_file_list;
	String res_filename;
	String orig_dir;

	public SortTask(List<String> mid_file_list, String res_filename, String orig_dir)
	{
		this.TaskType="Sort";
		this.TaskOutput = "Null";
		this.mid_file_list = mid_file_list;
		this.res_filename = res_filename;
		this.orig_dir = orig_dir;
	}
}
