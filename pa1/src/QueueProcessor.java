import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;

class QueueProcessor extends Thread
{
	ConcurrentLinkedQueue<Task> NodeTaskQueue;
	NodeHandler NodeItem;

	public QueueProcessor(NodeHandler NodeItem, ConcurrentLinkedQueue<Task> NodeTaskQueue)
	{
		this.NodeItem = NodeItem;
		this.NodeTaskQueue = NodeTaskQueue;
	}

	@Override
	public void run()
	{
		/*
		process each task in the queue (Using TaskProcessor)
		*/
		while(true)
		{
			try{
				Task CurrentTask=null;
				while(NodeTaskQueue.isEmpty())
				{
					Thread.sleep(50);
				}
				CurrentTask = NodeTaskQueue.remove();
				if(CurrentTask!=null)
				{
					TaskProcessor TaskProc = new TaskProcessor(CurrentTask);
					TaskProc.run();
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}

}