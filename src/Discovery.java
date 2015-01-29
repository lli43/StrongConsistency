import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Discovery {

	private Logger logger;
	
	private String primary;
	
	private Electing electing;
	
	Discovery() {
		logger = LogManager.getLogger(Discovery.class);
		primary = null;
		electing = new Electing();
	}
	
	
	public static void main(String[] args) {
		Discovery disc = new Discovery();
//		disc.getDataServerAddr();
		try {
			disc.serverStart(disc.electing);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			disc.logger.error("Cannot open the FrontEnd!");
		}
	}

	public void serverStart(Electing electing) throws Exception {

		int PORT = 5030;
		@SuppressWarnings("resource")
		ServerSocket serversock = new ServerSocket(PORT);
		logger.info("Discovery start!");
		ExecutorService executor = Executors.newFixedThreadPool(10);
//		WorkQueue workQueue = new WorkQueue(10);
		while (true) {
			Socket sock = serversock.accept();
			executor.execute(new DiscoveryProcess(sock, electing, logger));
		}
	}
	

}
