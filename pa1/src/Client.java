import java.io.File;
import java.util.Scanner;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

public class Client {
	ServerService.Client server;
    public static void main(String [] args) {
        //Create client connect.
    	if(args.length < 3) {
    	    System.err.println("Usage: java Client inputDir serverIP serverPort");
    	    return;
    	}
        try {
        	String serverIP = args[1];
        	int serverPort = Integer.parseInt(args[2]);
        	//address serverAdress = new Address(serverIP, serverPort);
        	String inputDir = args[0];
        	
            TTransport  transport = new TSocket(serverIP, serverPort);
    	    Client client = new Client();
    	    
            //Try to connect
    	    while(!client.connect(transport)) {
    		    System.err.println("Client: Failed to connect to Server, retrying");
    		    Thread.sleep(1000);
    	    }
    	    System.out.println("Contacted to server " + serverIP + ":" + serverPort);
    	    
            //What you need to do.
			File test=new File(inputDir);
			if(!test.exists())
			{
				System.out.println("Invalid input dir");
				return;
			}
    	    boolean result = client.run(inputDir);
    	    if (result) {
    	    	System.out.println("Job completed: Results are in ./results.");
    	    }else {
    	    	System.out.println("Job failed.");
    	    }
    	    
        } catch(Exception e) {
        	//e.printStackTrace();
        }

    }
    public boolean connect(TTransport  transport) {
    	try {
            transport.open();
    	    TProtocol serverProtocol = new TBinaryProtocol(new TFramedTransport(transport));
    	    server = new ServerService.Client(serverProtocol);
    	    return true;
    	}
    	catch(Exception e) {
    	    //e.printStackTrace();
    	    return false;
    	}
    	
    }
    public boolean run(String inputDir) {
    	try {
    		return server.getSentVal(inputDir);
    	}catch(Exception e) {
    	    e.printStackTrace();
    	    return false;    		
    	}
    }
}
