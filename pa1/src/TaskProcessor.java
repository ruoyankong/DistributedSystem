import com.sun.scenario.effect.Merge;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.io.FileWriter;


class TaskProcessor  extends Thread
{
	List<String> pos_word_list;
	List<String> neg_word_list;
	Task task;
	public TaskProcessor(Task _task)
	{
		this.task = _task;
	}

	@Override
	public void run()
	{
		/*
		Process a task
		 */
		try
		{   
			long StartTime = System.currentTimeMillis();
			Random RandomGenerator = new Random();
			int InjectLoadProb = RandomGenerator.nextInt(100);
			if(InjectLoadProb < task.LoadProb)
			{
				System.out.println("Injected Load");
				Thread.sleep(500);		//sleep 0.5s
			}
			if(task.TaskType=="Sort") {
				SortTask stask = (SortTask)task;
				Sort(stask);
				task.FillTaskOutput(stask.res_filename);
			}
			else if(task.TaskType=="Map") {
				MapTask mtask = (MapTask)task;
				Map(mtask);
				task.FillTaskOutput(mtask.mid_filename);
			}
			long EndTime = System.currentTimeMillis();

			task.TaskDuration = EndTime - StartTime;
			task.TaskNodeInfo = NodeHandler.NodeInfo;
			NodeHandler.announce(task);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public int CountWord_Hashmap(int mode, String filePath)
	{
		/*
		count positive/negative words in a file
		 */
		int res=0;
		try
		{
			String lineTxt = null;
			Map<String,Integer> WordDict = new HashMap<String, Integer>();
			File file = new File(filePath);
			InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "utf-8");
			BufferedReader br = new BufferedReader(isr);
			while ((lineTxt = br.readLine()) != null)
			{
				lineTxt = lineTxt.replaceAll("--|''|\t", " ");
				lineTxt = lineTxt.replace(';',' ');
				lineTxt = lineTxt.replace(',',' ');
				lineTxt = lineTxt.replace('.',' ');
				lineTxt = lineTxt.replace('?',' ');
				lineTxt = lineTxt.replace('!',' ');
				lineTxt = lineTxt.replace(':',' ');
				lineTxt = lineTxt.replace('(',' ');
				lineTxt = lineTxt.replace(')',' ');
				lineTxt = lineTxt.replace('[',' ');
				lineTxt = lineTxt.replace(']',' ');
				lineTxt = lineTxt.replace('|',' ');
				lineTxt = lineTxt.replaceAll(" '", " ");
				lineTxt = lineTxt.replaceAll("^'", " ");
				lineTxt = lineTxt.replaceAll("'", " ");
				lineTxt = lineTxt.replaceAll("'.*? ", " ");
				lineTxt = lineTxt.replaceAll("'.*?\n", "\n");
				lineTxt = lineTxt.toLowerCase();
				String[] lineList = lineTxt.split(" ");
				for(int i=0; i<lineList.length; i++)
				{
					Integer tmp = WordDict.get(lineList[i]);
					if(tmp==null)
						WordDict.put(lineList[i], 1);
					else
						WordDict.put(lineList[i], tmp.intValue()+1);
				}
			}
			br.close();
			if(mode==0)
			{
				for(String _s : pos_word_list)
				{
					Integer tmp = WordDict.get(_s);
					if(tmp!=null)
						res+=tmp.intValue();
				}
			}
			else if(mode==1)
			{
				for(String _s : neg_word_list)
				{
					Integer tmp = WordDict.get(_s);
					if(tmp!=null)
						res+=tmp.intValue();
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return(res);
	}


	public void Map(MapTask mtask)
	{
		/*
		process a Map task
		 */
		//MapTask mtask = (MapTask)_task;
		String filename=mtask.filename;
		String mid_filename = mtask.mid_filename;
		try
		{
			this.pos_word_list = NodeHandler.pos_word_list;
			this.neg_word_list = NodeHandler.neg_word_list;
			int PosCounter = CountWord_Hashmap(0, filename);
			int NegCounter = CountWord_Hashmap(1, filename);
			double SentimentScore = (double)(PosCounter - NegCounter) / (double)(PosCounter + NegCounter);
			System.out.println(PosCounter+"  "+NegCounter+" == "+SentimentScore);
			FileWriter fw = new FileWriter(mid_filename);
			fw.write(String.valueOf(SentimentScore));
			fw.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public void Sort(SortTask stask)
	{
		/*
		process a Sort task
		 */
		//SortTask stask = (SortTask)_task;
		List<String> mid_file_list = stask.mid_file_list;
		String res_filename = stask.res_filename;
		String _inputDir = stask.orig_dir;
		try
		{
			Files.deleteIfExists(Paths.get(res_filename));
			Map<String, Double> Dict = new HashMap<String, Double>();
			List<Map.Entry<String, Double>> DictList = new ArrayList<>();
			for(int i=0;i<mid_file_list.size();i++)
			{
				String lineTxt = null;
				File file = new File(mid_file_list.get(i));
				InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "utf-8");
				BufferedReader br = new BufferedReader(isr);
				lineTxt = br.readLine();
				br.close();
				double SentimentScore = Double.valueOf(lineTxt.toString());
				Dict.put(mid_file_list.get(i), SentimentScore);
			}
			for(Map.Entry<String, Double> DEntry : Dict.entrySet())
				DictList.add(DEntry);

			DictList.sort(new Comparator<Map.Entry<String, Double>>()
			{
				@Override
				public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2)
				{
					return(o2.getValue()>o1.getValue() ? 1 : -1);
				}
			});

			FileWriter fw = new FileWriter(res_filename);
			for(Map.Entry<String, Double> LEntry: DictList)
			{
				String tmp=String.valueOf(LEntry);
				tmp = tmp.replaceAll("../med_results", _inputDir);
				tmp = tmp.replace("=", " ");
				System.out.println(tmp);
				fw.write(tmp+"\n");
			}
			fw.close();

			for(String fp : mid_file_list)
				Files.deleteIfExists(Paths.get(fp));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
