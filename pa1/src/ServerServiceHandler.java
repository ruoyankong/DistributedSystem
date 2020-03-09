import org.apache.thrift.TException;
import java.util.Random;
import java.util.logging.*;
/**Logger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.FileHandler;
java.util.logging.SimpleFormatter***/


//import jdk.internal.jline.internal.ShutdownHooks.Task;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.util.stream.Collectors;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

public class ServerServiceHandler implements ServerService.Iface {
	String outputDir = "results";
	String intermediateDir = "med_results";
	CopyOnWriteArrayList<Address> currentNodes;
	ConcurrentLinkedQueue<Task> currentTasks;
	Map<Address, ConcurrentLinkedQueue<Task>> nodeTasks;
	int n_tasks = 0;
	int stat_n_tasks = 0;
	int stat_n_nodes = 0;
	Long stat_map_time = (long) 0;
	Long stat_reduce_time = (long) 0;
	ArrayList<Integer> stat_fail_prob = new ArrayList<>();
	Random rand = new Random();
	Integer n_complete_tasks;
	private static Logger LOGGER = Logger.getLogger("InfoLogging");
	Handler handlerObj = new ConsoleHandler();
	FileHandler fh = new FileHandler("Logging.log", false);  
	SimpleFormatter sf = new SimpleFormatter();

	public ServerServiceHandler() throws Exception {
		handlerObj.setLevel(Level.WARNING);
		LOGGER.addHandler(handlerObj);
		fh.setFormatter(sf);
		LOGGER.addHandler(fh);
		LOGGER.setUseParentHandlers(false);
		currentNodes = new CopyOnWriteArrayList<Address>();
		currentTasks = new ConcurrentLinkedQueue<Task>();
		nodeTasks = new HashMap<>();
		Path outputPath = Paths.get("..", outputDir);
		Files.createDirectories(outputPath);
		Path intermediatePath = Paths.get("..", intermediateDir);
		Files.createDirectories(intermediatePath);
		this.n_complete_tasks = 0;
	}

	@Override
	public boolean addNode(Address address) throws TException {
		this.currentNodes.add(address);
		System.out.println("Node " + address.ip + "added.");
		return true;
	}

	@Override
	public boolean update(Address node, String taskFilename, long duration, String taskType) throws TException {
		synchronized (n_complete_tasks) {
			n_complete_tasks++;
			LOGGER.info(taskType +" Task "+taskFilename+" assigned to node "+node.ip+": "+node.port
					+" succeed in "+duration* 1.0 /1000+" seconds.");
			System.out.println(taskType +" Task "+taskFilename+" assigned to node "+node.ip+": "+node.port
					+" succeed in "+duration* 1.0 /1000+" seconds.");
			System.out.println("n_complete_tasks:" + this.n_complete_tasks);
		}
		return true;
	}

	@Override
	public boolean getSentVal(String inputDir) throws TException {
		Address node;
		Long map_begin_time = System.currentTimeMillis();
		Path inputPath = Paths.get(inputDir);
		Path intermediatePath = Paths.get("..", this.intermediateDir);
		ConcurrentLinkedQueue<Task> nodeTasks_tmp;
		ArrayList<String> mid_file_list = new ArrayList<>();
		try {
			List<File> listOfFiles = Files.walk(Paths.get(inputDir))
					.filter(Files::isRegularFile)
					.map(Path::toFile)
					.collect(Collectors.toList());
			for (int i = 0; i < listOfFiles.size(); i++) {
				if (listOfFiles.get(i).isFile()) {
					MapTask maptask = new MapTask(inputPath.resolve(listOfFiles.get(i).getName()).toString(),
							intermediatePath.resolve(listOfFiles.get(i).getName()).toString());
					maptask.TaskOutput = intermediatePath.resolve(listOfFiles.get(i).getName()).toString();
					this.currentTasks.add(maptask);
				}
			}
			this.stat_n_tasks = this.currentTasks.size();
			LOGGER.info("Get " + this.currentTasks.size() + " map tasks.");
			System.out.println("Get " + this.currentTasks.size() + " map tasks.");
		} catch (IOException e) {
			System.out.println("IO error while loading files");
			System.exit(-1);
		}
		this.n_tasks = this.currentTasks.size();
		this.stat_n_nodes = this.currentNodes.size();
		System.out.println("Current number of nodes: " + this.stat_n_nodes);
		LOGGER.info("Current number of nodes: " + this.stat_n_nodes);
		for (Task t : this.currentTasks) {
			MapTask task = (MapTask) this.currentTasks.poll();
			if (task != null) {
				node = this.currentNodes.get(rand.nextInt(this.currentNodes.size()));
				try {
					if (!this.doMapTask(node, task)) {
						LOGGER.info("Map Task "+task.mid_filename+" assigned to node "+node.ip+": "+node.port
								+" is rejected. Task will be reassigned.");
						System.out.println("Map Task "+task.mid_filename+" assigned to node "+node.ip+": "+node.port
								+" is rejected. Task will be reassigned.");
						System.out.println("n_complete_tasks:" + this.n_complete_tasks);
						this.currentTasks.add(task);
					} else {
						mid_file_list.add(task.mid_filename);
					}
				} catch (Exception e) {
					LOGGER.info("Map Task "+task.mid_filename+" assigned to node "+node.ip+": "+node.port
							+" is rejected. Task will be reassigned.");
					System.out.println("Map Task "+task.mid_filename+" assigned to node "+node.ip+": "+node.port
							+" is rejected. Task will be reassigned.");
					System.out.println("n_complete_tasks:" + this.n_complete_tasks);
					this.currentTasks.add(task);
				}
			}
		}

		while (n_complete_tasks < this.n_tasks) {
			MapTask task = (MapTask) this.currentTasks.poll();
			if (task != null) {
				node = this.currentNodes.get(rand.nextInt(this.currentNodes.size()));
				try {
					if (!this.doMapTask(node, task)) {
						LOGGER.info("Map Task "+task.mid_filename+" assigned to node "+node.ip+": "+node.port
								+" is rejected. Task will be reassigned.");
						System.out.println("Map Task "+task.mid_filename+" assigned to node "+node.ip+": "+node.port
								+" is rejected. Task will be reassigned.");
						System.out.println("n_complete_tasks:" + this.n_complete_tasks);
						this.currentTasks.add(task);
					} else {
						mid_file_list.add(task.mid_filename);
					}

				} catch (Exception e) {
					LOGGER.info("Map Task "+task.mid_filename+" assigned to node "+node.ip+": "+node.port
							+" is rejected. Task will be reassigned.");
					System.out.println("Map Task "+task.mid_filename+" assigned to node "+node.ip+": "+node.port
							+" is rejected. Task will be reassigned.");
					System.out.println("n_complete_tasks:" + this.n_complete_tasks);
					this.currentTasks.add(task);
				}
			}
		}
		Long map_end_time = System.currentTimeMillis();
		LOGGER.info("The map tasks above completed. Begin Sort Tasks.");
		System.out.println("The map tasks above completed. Begin Sort Tasks.");
		Path outputPath = Paths.get("..", outputDir);
		this.n_complete_tasks = 0;
		this.currentTasks = new ConcurrentLinkedQueue<Task>();
		SortTask sorttask = new SortTask(mid_file_list, outputPath.resolve("result_file").toString(), inputPath.toString());
		sorttask.TaskOutput = outputPath.resolve("result_file").toString();
		this.currentTasks.add(sorttask);
		System.out.println("Get " + 1 + " sort tasks.");
		LOGGER.info("Get " + 1 + " sort tasks.");
		this.n_tasks = 1;
		sorttask = (SortTask) this.currentTasks.poll();
		if (sorttask != null) {
			node = this.currentNodes.get(rand.nextInt(this.currentNodes.size()));
			try {
				if (!this.doSortTask(node, sorttask)) {
					LOGGER.info("Sort Task "+sorttask.res_filename+" assigned to node "+node.ip+": "+node.port
							+" is rejected. Task will be reassigned.");
					System.out.println("Sort Task "+sorttask.res_filename+" assigned to node "+node.ip+": "+node.port
							+" is rejected. Task will be reassigned.");
					System.out.println("n_complete_tasks:" + this.n_complete_tasks);
					this.currentTasks.add(sorttask);
				}
			} catch (Exception e) {
				LOGGER.info("Sort Task "+sorttask.res_filename+" assigned to node "+node.ip+": "+node.port
						+" is rejected. Task will be reassigned.");
				System.out.println("Sort Task "+sorttask.res_filename+" assigned to node "+node.ip+": "+node.port
						+" is rejected. Task will be reassigned.");
				System.out.println("n_complete_tasks:" + this.n_complete_tasks);
				this.currentTasks.add(sorttask);
			}
		}
		while (n_complete_tasks < this.n_tasks) {
			sorttask = (SortTask) this.currentTasks.poll();
			;
			if (sorttask != null) {
				node = this.currentNodes.get(rand.nextInt(this.currentNodes.size()));
				try {
					if (!this.doSortTask(node, sorttask)) {
						LOGGER.info("Sort Task "+sorttask.res_filename+" assigned to node "+node.ip+": "+node.port
								+" is rejected. Task will be reassigned.");
						System.out.println("Sort Task "+sorttask.res_filename+" assigned to node "+node.ip+": "+node.port
								+" is rejected. Task will be reassigned.");
						System.out.println("n_complete_tasks:" + this.n_complete_tasks);
						this.currentTasks.add(sorttask);
					}
				} catch (Exception e) {
					LOGGER.info("Sort Task "+sorttask.res_filename+" assigned to node "+node.ip+": "+node.port
							+" is rejected. Task will be reassigned.");
					System.out.println("Sort Task "+sorttask.res_filename+" assigned to node "+node.ip+": "+node.port
							+" is rejected. Task will be reassigned.");
					System.out.println("n_complete_tasks:" + this.n_complete_tasks);
					this.currentTasks.add(sorttask);
				}
			}
		}
		Long reduce_end_time = System.currentTimeMillis();
		System.out.println("The sort tasks above completed in " + outputPath.resolve("result_file").toString());
		LOGGER.info("The sort tasks above completed in " + outputPath.resolve("result_file").toString());
		this.stat_map_time = map_end_time - map_begin_time;
		this.stat_reduce_time = reduce_end_time - map_end_time;
		System.out.println("-----------statistics-----------");
		System.out.println("number of map tasks: " + this.stat_n_tasks);
		System.out.println("number of sort tasks: " + this.n_tasks);
		System.out.println("number of nodes: " + this.stat_n_nodes);
		System.out.println("map time (seconds): " + this.stat_map_time* 1.0 /1000);
		System.out.println("reduce time (seconds): " + this.stat_reduce_time* 1.0 /1000);
		LOGGER.info("-----------statistics-----------");
		LOGGER.info("number of map tasks: " + this.stat_n_tasks);
		LOGGER.info("number of sort tasks: " + this.n_tasks);
		LOGGER.info("number of nodes: " + this.stat_n_nodes);
		LOGGER.info("map time (seconds): " + this.stat_map_time* 1.0 /1000);
		LOGGER.info("reduce time (seconds): " + this.stat_reduce_time* 1.0 /1000);

		return true;

	}

	private boolean doMapTask(Address node, MapTask task) throws TException {
		boolean success = false;
		try {
			TTransport transport = new TSocket(node.ip, node.port);
			transport.open();
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(transport));
			NodeService.Client nodeService = new NodeService.Client(serverProtocol);
			success = nodeService.AddMapTask(task.filename, task.mid_filename);
			transport.close();
			return success;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	private boolean doSortTask(Address node, SortTask task) throws TException {
		boolean success = false;
		try {
			TTransport transport = new TSocket(node.ip, node.port);
			transport.open();
			TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(transport));
			NodeService.Client nodeService = new NodeService.Client(serverProtocol);
			success = nodeService.AddSortTask(task.mid_file_list, task.res_filename, task.orig_dir);
			transport.close();
			return success;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}
}
