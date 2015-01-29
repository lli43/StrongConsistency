import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class DataServer extends Thread {

	private MultiReaderLock lock = new MultiReaderLock();
	private Logger logger = LogManager.getLogger(DataServer.class);
	private final static String LINE = "\r\n";
	private String HOST = "localhost";
	private int PORT = 5010;
	private int priority = Integer.MAX_VALUE;
	private String primary;
	private String disHost = "mc07";
	private int disPort = 5030;
	private DataServerState dataSS = new DataServerState();
//	private Timer timer = new Timer(true);
	private JSONObject dataservers; 
	private int pending = 0;
	private DataMaintenance dataM = new DataMaintenance(logger);

	DataServer() {
		
	}
	DataServer(String host, String port) {
		this.HOST = host;
		this.PORT = Integer.parseInt(port);
	}
	public static void main(String[] args) {
		DataServer dataServer;
		if (args.length > 0) {
			dataServer = new DataServer(args[0], args[1]);
		} else {
			dataServer = new DataServer();
		}
		dataServer.start();
		// System.out.println("hhhhh");
		try {
			dataServer.serverStart();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			dataServer.logger.error("DataServer cannot start!");
		}
	}

	/*
	 * Start the dataserver
	 */
	public void serverStart() throws Exception {

		dataSS.setHost(HOST, PORT);
		dataSS.setState("NEW");
		@SuppressWarnings("resource")
		ServerSocket serversock = new ServerSocket(PORT);
		logger.info("DataServer start!");
		
		ExecutorService executor = Executors.newFixedThreadPool(20);
		// WorkQueue workQueue = new WorkQueue(10);
		while (true) {
			Socket sock = serversock.accept();
			executor.execute(new DSRequestProcessor(sock, dataM, dataSS, logger));
		}
	}

	
	/*
	 * At first, new data server need to get primary from the discovery. If there is
	 * no primary, this data server start election
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		logger.info("register!");
		register();
		if (primary.equals("")) {
			getDataServers();
			election();
		} else {
			dataSS.setPrimary(primary);
			String u = "/primary/addSecondery";
			String b = HOST + ":" + PORT;
			String[] primaryAddr = dataSS.getPrimary().split(":");
			String response = sendAndGetMess("GET", u, b, primaryAddr[0], Integer.parseInt(primaryAddr[1]));
//			if (response == null) {
//			}
			while (dataSS.getState().equals("NEW")) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			detectingPrimary();
		}
	}

	/*
	 * This function is used to add this data server to the list in discovery server
	 */
	public void register() {
		String uri = "/discovery/getPrimary?host=dataserver";
		logger.info(dataSS.getHost() + ":" + dataSS.getPort() + "register");
		String sendBody = null;
		String body = sendAndGetMess("GET" ,uri, sendBody, disHost, disPort);
		
		JSONObject jobj = new JSONObject();
		try {
			JSONParser jParser = new JSONParser();
			Object obj = jParser.parse(body);
			jobj = (JSONObject) obj;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		priority = Integer.parseInt(jobj.get("priority").toString());
		primary = jobj.get("primary").toString();
	}
	
	/*
	 * 	Send message to particular target and get some content
	 */
	public String sendAndGetMess(String method, String uri, String sendBody, String disHost, int disPort) {
		logger.debug(HOST + ":" + PORT + " WITH " + disHost + ":" + disPort + " Connection starts!");
		String httpversion = "HTTP/1.1";
		try {
			uri = URLEncoder.encode(uri, "utf-8");
		} catch (UnsupportedEncodingException e2) {
			e2.getMessage();
		}
		StringBuffer header = new StringBuffer();
		header.append(method + " " + uri + " " + httpversion + LINE);
		header.append("Host: " + HOST + ":" + PORT + LINE);
		if (sendBody != null) {
			header.append("Content-Length : " + sendBody.getBytes().length + LINE);
		}
		header.append(LINE);
		if (sendBody != null) {
			header.append(sendBody);
		}
		String requestLine = "";
		int content_Length = 0;
		String body = null;
		try {
			Socket sock = new Socket(disHost, disPort);
			OutputStream out;
			out = sock.getOutputStream();
			out.write(header.toString().getBytes());
			out.flush();

			BufferedReader in;
			in = new BufferedReader(
					new InputStreamReader(sock.getInputStream()));
			String line;

			while (!(line = in.readLine().trim()).equals("")) {
//				System.out.println(line);
				requestLine = requestLine + line + "\n";
				if (line.startsWith("Content-Length")) {
					String[] l = line.split(":");
					content_Length = Integer.parseInt(l[1].trim());
				}
			}
//			 System.out.println(requestLine);
			// System.out.println(content_Length);
			/*
			 * Get POST body if exists
			 */
			if (in.ready()) {
				char[] h = new char[content_Length];
				in.read(h);
				StringBuffer sb = new StringBuffer();
				sb.append(h);
				body = sb.toString().trim();
//				 System.out.println(body);
			}
			sock.close();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			logger.debug(HOST + ":" + PORT + " WITH " + disHost + ":" + disPort + " Connection is broken");
			System.out.println(HOST + ":" + PORT + " WITH " + disHost + ":" + disPort + " Connection is broken");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.debug(HOST + ":" + PORT + " WITH " + disHost + ":" + disPort + " Connection is broken");
			System.out.println(HOST + ":" + PORT + " WITH " + disHost + ":" + disPort + " Connection is broken");
		}
		return body;
	}
	/*
	 * get the all data servers from discovery server
	 */
	public void getDataServers() {
		logger.info(HOST + ":" + PORT + "get data server list from discovery server");
		String uri = "/discovery/getDataServerList";
		String body = sendAndGetMess("GET", uri, null, disHost, disPort);
		JSONObject jobj = new JSONObject();
		try {
			JSONParser jParser = new JSONParser();
			Object obj = jParser.parse(body);
			jobj = (JSONObject) obj;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		dataservers = jobj;
	}

	/*
	 * When the secondery find the primary is broken, it start to elect a new primary.
	 * The secondery first get the data server list from discovery server, then, it 
	 * send are you alive message to the server whose priority larger then its. If no
	 * response, the secondery knows that it can be primary, call trytoBePrimary() 
	 * function. If there is any response from the higher one, the secondery will halt
	 * then wait for the new primary's broadcast.
	 */
	public void election() {
		dataSS.setState("ELECTING");
		System.out.println("ELECTION");
		logger.info("Start Election!");
		boolean canbeprimary = true;
		/*
		 * Check the all data servers whose priority is larger than mine. Send the are you alive to the 
		 * data server, if get answer, I quit competition for primary.
		 */
		for (Object key : dataservers.keySet()) {
			String dataServerAddr = (String) key;
			if (!dataServerAddr.equals(HOST + ":" + PORT)) {
				int p = Integer.parseInt(dataservers.get(key).toString());
				if (p < priority) {
					if (sendAreUAlive(dataServerAddr)) {
						canbeprimary = false;
						break;
					}
				}
			}
		}
		if (canbeprimary) {
			trytoBePrimary();
		} else {
			waitingforNewP();
		}
	}
	/*
	 * When the secondery know it can be the primary, it call this function.
	 * Broadcast to the all other data servers, after that, it can be the 
	 * primary.
	 */
	public void trytoBePrimary() {
		logger.info(HOST + ":" + PORT + "try to be primary server");
		ExecutorService executor = Executors.newFixedThreadPool(10);
		for (Object key : dataservers.keySet()) {
			String dataServerAddr = (String) key;
			if (!dataServerAddr.equals(HOST + ":" + PORT)) {
				String addr[] = dataServerAddr.split(":");
//				executor.execute(new Tobeprimary(addr[0], Integer.parseInt(addr[1])));
				String uri = "/dataserver/Iamprimary";
				JSONObject obj = new JSONObject();
				obj.put("primary", HOST + ":" + PORT);
				obj.put("priority", priority);
				String body = sendAndGetMess("GET", uri, obj.toJSONString(), addr[0], Integer.parseInt(addr[1]));
				if (body != null) {
					if (body.equals("OK")){
						if (!dataSS.getAllseconderies().contains(addr[0] + ":" + Integer.parseInt(addr[1])))
							dataSS.addSecondery(addr[0] + ":" + Integer.parseInt(addr[1]));
					}
				}
			}
		}
//		finish();
		dataSS.setIsprimary(true);
		String uri = "/discovery/iamprimary";
		JSONObject obj = new JSONObject();
		obj.put("primary", HOST + ":" + PORT);
		System.out.println(obj.toJSONString());
		consistency();
		String body = sendAndGetMess("GET", uri, obj.toJSONString(), disHost, disPort);
		dataSS.setState("NORMAL");
	}
	/*
	 * After election, the primary start to do consistency. At first, primary get the totalVersion number
	 * from the all other data servers. Compare the totalVersions, get the max one and the min one. get 
	 * different by (max - min), get the different of the versions from the max version machine. Send the 
	 * different data to all servers.
	 */
	public void consistency() { 
//		String newestOne = null;
//		String oldestOne = null;
//		boolean newest = true;
//		boolean needCons = false;
		logger.info("Start to do the consistency!");
		String uri = "/seconderies/getTotalVersion";
		ArrayList<String> secArray = dataSS.getAllseconderies();
		TreeMap<Integer, String> tree = new TreeMap<Integer, String>();
		logger.info("Get the different versions from the all dataservers!");
		for (String addre : secArray) {
			String[] address = addre.split(":");
			String response = sendAndGetMess("GET", uri, null, address[0], Integer.parseInt(address[1]));
			JSONObject jobj = new JSONObject();
			try {
				JSONParser jParser = new JSONParser();
				Object obj = jParser.parse(response);
				jobj = (JSONObject)obj;
			}
			/*
			 * If it is not a JSON, return 400
			 */
			catch (ParseException e) {
				// TODO Auto-generated catch block
				logger.error("The post body from FrontEnd is wrong!");
			}
			int secVer = Integer.parseInt(jobj.get("totalVersion").toString());
			tree.put(secVer, addre);
		}
		tree.put(dataM.getTotalVersion(), "self");
		if (tree.size() == 1) {
			return;
		}
		executeConsistency(tree);
	}
	
	/*
	 * Use the treeMap, I can know the max and min directly.
	 * @param tree
	 */
	public void executeConsistency(TreeMap<Integer, String> tree) {
		int different = tree.lastKey() - tree.firstKey();
		JSONObject obj = new JSONObject();
		obj.put("different", different);
		String uri = "/seconderies/getNewest";
		String method = "GET";
		String newest = null;
		JSONObject differentData = new JSONObject();
		if (!tree.lastEntry().getValue().equals("self")) {
			String[] target = tree.lastEntry().getValue().split(":");
			newest = sendAndGetMess(method, uri, obj.toJSONString(), target[0], Integer.parseInt(target[1]));
			try {
				JSONParser jParser = new JSONParser();
				Object obj1 = jParser.parse(newest);
				differentData = (JSONObject)obj1;
			}
			catch (ParseException e) {
				// TODO Auto-generated catch block
				logger.error("The post body from FrontEnd is wrong!");
			}
			dataM.updataDifferentdata(differentData);
		} else {
			
			differentData.put("data", dataM.getDifferentLog(different));
			differentData.put("totalVersion", dataM.getTotalVersion());
		}
		sendDifferentData(differentData);
	}
	
	/*
	 * Send the different DATA to all servers.
	 */
	public void sendDifferentData(JSONObject differentData) {
		logger.info("Send the different data to all data servers.");
		String uri = "/seconderies/updataDiff";
		String method = "POST";
		ArrayList<String> secArray = dataSS.getAllseconderies();
		for (String sec : secArray) {
			String[] address = sec.split(":");
			String response = 
					sendAndGetMess(method, uri, differentData.toJSONString(), 
							address[0], Integer.parseInt(address[1]));
		}
	}
	
	public class Tobeprimary implements Runnable {
		private String host;
		private int port;
		Tobeprimary(String host, int port) {
			this.host = host;
			this.port = port;
			incrementPending();
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			String uri = "/dataserver/Iamprimary";
			JSONObject obj = new JSONObject();
			obj.put("primary", HOST + ":" + PORT);
			obj.put("priority", priority);
			String body = sendAndGetMess("GET", uri, obj.toJSONString(), host, port);
			if (body != null) {
				if (body.equals("OK")){
					if (!dataSS.getAllseconderies().contains(host + ":" + port))
						dataSS.addSecondery(host + ":" + port);
				}
			}
			System.out.println(body);
			decrementPending();
		}
	}
	
	/*
	 * Waiting until the data server get a new primary.
	 */
	public void waitingforNewP() {
		logger.info(HOST + ":" + PORT + " waiting for higher priority's data server being primary");
		dataSS.setState("WAITING");
		while (dataSS.getPrimary() == null) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		detectingPrimary();
	}
	
	/*
	 * detect the primary that if the primary is crashed. Send a are you alive message 
	 * every 3 seconds.
	 */
	public void detectingPrimary() {
		logger.info(HOST + ":" + PORT + " detection the primary if it is failed");
		String[] primaryAddr = dataSS.getPrimary().split(":");
		String uri = "/primary/detection";
		dataSS.setState("NORMAL");
		while (true) {
			if (sendAndGetMess("GET", uri, null, primaryAddr[0], Integer.parseInt(primaryAddr[1])) == null) {
				break;
			}
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
		dataSS.setPrimary(null);
		getDataServers();
		election();
	}
	
	public boolean sendAreUAlive(String dataServerAddr) {
		boolean isalive = false;
		String addr[] = dataServerAddr.split(":");
		String host = addr[0];
		int port = Integer.parseInt(addr[1]);
		String uri = "/dataserver/areualive";
		String response = sendAndGetMess("GET", uri, null, host, port);
		if (response != null) {
			if (response.equals("YES")) {
				isalive = true;
			}
		}
		return isalive;
	}
	
	private void incrementPending() {
		lock.lockWrite();
		pending++;
		lock.unlockWrite();
		logger.debug("Pending is now {}" + pending);
	}

	/**
	 * Indicates that we now have one less "pending" work, and will notify any
	 * waiting threads if we no longer have any more pending work left.
	 */
	private void decrementPending() {
		lock.lockWrite();
		pending--;
		lock.unlockWrite();
		logger.debug("Pending is now {}", pending);
	}

	/**
	 * if the pending is larger than 0, means that there is still thread running
	 * so we lock it
	 */
	public synchronized void finish() {
		while (getPending() > 0) {
			System.out.println("+++++++++" + pending);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.debug("Waiting until finished");
		}
	}
	
	private int getPending() {
		lock.lockRead();
		int i = pending;
		lock.unlockRead();
		return i;
	}
}
