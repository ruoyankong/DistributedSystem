import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.*;

import javax.xml.soap.Node;
import java.io.*;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.LinkedList;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

public class NodeHandler implements NodeService.Iface
{
        ConcurrentLinkedQueue<Task> TaskQueue;
        double LoadProb = 0.0;
        int mode;
        static Address NodeInfo;
        static Address ServerInfo;
        static List<String> pos_word_list = new ArrayList<>();
        static List<String> neg_word_list = new ArrayList<>();

        @Override
        public boolean ping() throws TException 
        {
            return(true);
        }

        public void SaveWordLiist(int mode, String _WordListDir)
        {
            /*
            Read positive/negative word list
            mode==1: negative list
            mode==2: positive list
             */
            System.out.println("Reading Word List");
            try
            {
                String lineTxt = null;
                File file = new File(_WordListDir);
                InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "utf-8");
                BufferedReader br = new BufferedReader(isr);
                while ((lineTxt = br.readLine()) != null)
                {
                    if(mode==1)
                        neg_word_list.add(lineTxt);
                    else if(mode==0)
                        pos_word_list.add(lineTxt);
                }
                br.close();
            }
            catch (IOException e) {
                System.out.println("IO error while loading word list");
                e.printStackTrace();
                System.exit(-1);
            }
        }

        //@Override
        public void InitNode(int mode, double LoadProb, String ServerIP, int ServerPort, int NodePort) throws TException
        {
        	/*
        	Initialize this node, and make connection to server
        	mode: set working mode of compute node
        	    1: random scheduling
        		2: load-balancing
        	LoadProb: set Load Probability in load-balancing mode
        	*/
        	try {
                this.LoadProb = LoadProb;
                this.mode = mode;
                TTransport ServerT = new TSocket(ServerIP, ServerPort);
                ServerT.open();
                TProtocol ServerP = new TBinaryProtocol(new TFramedTransport(ServerT));
                ServerService.Client ServerClient = new ServerService.Client(ServerP);

                NodeInfo = new Address();
                NodeInfo.ip = InetAddress.getLocalHost().getHostName().toString();
                NodeInfo.port = NodePort;

                ServerInfo = new Address();
                ServerInfo.ip = ServerIP;
                ServerInfo.port = ServerPort;

                String NegWordListDir = "../data/negative.txt";
                String PosWordListDir = "../data/positive.txt";
                SaveWordLiist(1, NegWordListDir);
                SaveWordLiist(0, PosWordListDir);

               // ServerClient.update(NodeInfo, "file");
                boolean res = ServerClient.addNode(NodeInfo);
                TaskQueue = new ConcurrentLinkedQueue<>();
                if (res)
                    System.out.println("Node has connected to server ");
                else
                    System.out.println("Node could not connect to server");
                ServerT.close();
            }
        	catch (Exception e)
            {
                System.out.println("Failed to connect to server");
                System.exit(-1);
            }
        }

		@Override
        public boolean AddMapTask(String filename, String mid_filename)
        {
            /*
            Add a Map task into Queue
             */
			boolean success=false;
			try
			{
	            MapTask task = new MapTask(filename, mid_filename);
	            
				Random RandomGenerator = new Random();
				int InjectLoadProb = RandomGenerator.nextInt(100);
				task.LoadProb = (int)(this.LoadProb*100);

				if(InjectLoadProb < (int)(100*this.LoadProb) && this.mode == 2)
				{	//FailThisTask(CurrentTask)
                    System.out.println("Failed Map Task at "+filename);
					return false;
				}
				else
				{
		            synchronized(TaskQueue)
		            {
		                TaskQueue.add(task);
		                success=true;
		            }
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
            return success;
        }

        @Override
        public boolean AddSortTask(List<String> mid_file_list, String res_filename, String orig_dir)
        {
            /*
            Add a Sort task into Queue
             */
        	boolean success=false;
        	try
        	{
	        	SortTask task = new SortTask(mid_file_list, res_filename, orig_dir);

                Random RandomGenerator = new Random();
                int InjectLoadProb = RandomGenerator.nextInt(100);
                task.LoadProb = (int)(this.LoadProb*100);

				if(InjectLoadProb < (int)(100*this.LoadProb) && this.mode == 2)
				{	//FailThisTask(CurrentTask)
				    System.out.println("Failed Sort Task");
					return false;
				}
				else
				{
		            synchronized(TaskQueue)
		            {
		                TaskQueue.add(task);
		                success=true;
		            }
				}
        	}
			catch (Exception e)
			{
                e.printStackTrace();
			}
            return success;
        }

        public void StartNode() throws TException
        {
        	/*
        	run the compute node
        	process task queue (using QueueProcessor)
        	*/
        	try {
                QueueProcessor QP = new QueueProcessor(this, TaskQueue);
                QP.start();

                TServerTransport ServeTransport = new TServerSocket(NodeInfo.port);
                TTransportFactory TransportFactory = new TFramedTransport.Factory();

                NodeService.Processor processor = new NodeService.Processor<>(this);

                TThreadPoolServer.Args ServerArgs = new TThreadPoolServer.Args(ServeTransport);
                ServerArgs.processor(processor);
                ServerArgs.transportFactory(TransportFactory);

                TServer Server = new TThreadPoolServer(ServerArgs);
                Server.serve();
            }
        	catch (Exception e)
            {
                e.printStackTrace();

            }
        }

        public static void announce(Task _task) throws TException
        {
            /*
            Announce to server that the task has been successfully completed
             */
            TTransport ServerTransport = new TSocket(ServerInfo.ip, ServerInfo.port);
            ServerTransport.open();
            TProtocol ServerProtocol = new TBinaryProtocol(new TFramedTransport(ServerTransport));
            ServerService.Client Server = new ServerService.Client(ServerProtocol);
            Server.update(NodeInfo, _task.TaskOutput, _task.TaskDuration, _task.TaskType);
            ServerTransport.close();
        }

		public static void main(String[] args)
		{

            if(args.length<5)
            {
                System.out.println("Usage: java NodeHandler <Mode> <ServerIP> <ServerPort> <NodePort> <LoadProb>");
                return;
            }
            try
            {
                System.out.println("IP Address of this node: "+InetAddress.getLocalHost().toString());
                String ServerIP = args[1];
                Integer ServerPort = Integer.parseInt(args[2]);
                Integer NodePort = Integer.parseInt(args[3]);
                Integer mode = Integer.parseInt(args[0]);
                double LoadProb = new Double(args[4]).doubleValue();

                NodeHandler NodeItem = new NodeHandler();
                NodeItem.InitNode(mode, LoadProb, ServerIP, ServerPort, NodePort);
                NodeItem.StartNode();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
		}
}
