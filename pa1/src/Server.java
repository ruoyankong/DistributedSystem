import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.server.*;
public class Server {

    public static void main(String [] args) {
        /***TServerTransport serverTransport = new TServerSocket(9090);
        TTransportFactory factory = new TFramedTransport.Factory();

        //Create service request handler
        ServerServiceHandler handler = new ServerServiceHandler();
        processor = new ServerService.Processor(handler);

        //Set server arguments
        TServer.Args args = new TServer.Args(serverTransport);
        args.processor(processor);  //Set handler
        args.transportFactory(factory);  //Set FramedTransport (for performance)

        //Run server as a single thread
        TServer server = new TSimpleServer(args);
        server.serve();***/
        if(args.length<1)
        {
            System.out.println("Usage: java Server <ServerPort>");
            return;
        }
        try {
        Integer ServerPort = Integer.parseInt(args[0]);
        TServerTransport serverTransport = new TServerSocket(ServerPort);
        TTransportFactory factory = new TFramedTransport.Factory();
        ServerServiceHandler handler = new ServerServiceHandler();
        ServerService.Processor processor = new ServerService.Processor<>(handler);

        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
        serverArgs.processor(processor); 
        serverArgs.transportFactory(factory); 

        TServer server = new TThreadPoolServer(serverArgs);
        server.serve();
        }catch(TException e) {
        	e.printStackTrace();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

  /***  public static void simple(Serv) {
        try {
            //Create Thrift server socket
            /***TServerTransport serverTransport = new TServerSocket(9090);
            TTransportFactory factory = new TFramedTransport.Factory();

            //Create service request handler
            ServerServiceHandler handler = new ServerServiceHandler();
            processor = new ServerService.Processor(handler);

            //Set server arguments
            TServer.Args args = new TServer.Args(serverTransport);
            args.processor(processor);  //Set handler
            args.transportFactory(factory);  //Set FramedTransport (for performance)

            //Run server as a single thread
            TServer server = new TSimpleServer(args);
            server.serve();
            
            //Create Thrift server socket
            TServerTransport serverTransport = new TServerSocket(9090);
            TTransportFactory factory = new TFramedTransport.Factory();
            Server.Processor processor = new Server.Processor<>(this);

            //Set Server Arguments
            TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
            serverArgs.processor(processor); //Set handler
            serverArgs.transportFactory(factory); //Set FramedTransport (for performance)

            //Run server with multiple threads
            TServer server = new TThreadPoolServer(serverArgs);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }***/
}

