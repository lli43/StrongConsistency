import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URLEncoder;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class DiscoveryProcess implements Runnable {

	private Socket sock;
	private final static String LINE = "\r\n";
	private Logger logger;
	private Electing elect;
	private String host;
	private String hostAddre;
	private int hostPort;
	public DiscoveryProcess(Socket sock, Electing elect, Logger logger) {
		this.sock = sock;
		this.elect = elect;
		this.logger = logger;
	}

	@Override
	public void run() {

		HTTPRequestLine httprequestLine = new HTTPRequestLine();
		/*
		 * Get the first line of client request
		 */
		BufferedReader in;
		String requestLine = "";
		String body = "";
		int content_Length = 0;
		try {
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
				if (line.startsWith("Host:")) {
					String[] str = line.split(" ");
					host = str[1].trim();
					String[] st = host.split(":");
					hostAddre = st[0];
					hostPort = Integer.parseInt(st[1]);
				}
			}
//			System.out.println(requestLine);
//			System.out.println(host+hostAddre+hostPort);
//			System.out.println(content_Length);
			/*
			 * Get POST body if exists
			 */
			if (in.ready()) {
				char[] h = new char[content_Length];
				in.read(h);
				StringBuffer sb = new StringBuffer();
				sb.append(h);
				body = sb.toString().trim();
//				System.out.println(body);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("Client is closed!");
			return;
		}

		/*
		 * Parse the request line, if it is not a valid one, I will get null
		 */
		httprequestLine = HTTPRequestLineParser.parse(requestLine, logger);
		if (httprequestLine == null) {
			System.out.println("It's a invalid request line.");
			return;
		}
		String uri = httprequestLine.getUripath();
//		System.out.println(uri);
		if (uri.equals("/discovery/getPrimary")) {
//			System.out.println("true");
			execution(httprequestLine);
			return;
		}
		if (uri.equals("/discovery/getDataServerList")) {
			sendDataServerlist();
			return;
		}
		if (uri.equals("/discovery/iamprimary")) {
			setPrimary(body);
			return;
		}
	}
	
	public void setPrimary(String body) {
		JSONObject jobj = new JSONObject();
		try {
			JSONParser jParser = new JSONParser();
			Object obj = jParser.parse(body);
			jobj = (JSONObject) obj;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String primary = jobj.get("primary").toString();
		elect.setPrimary(primary);
		System.out.println(elect.getPrimary());
		StringBuffer sb = new StringBuffer();
		String responsebody = "OK!";
		String responseheader = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responsebody.getBytes().length + "\n\n";
		sb.append(responseheader);
		sb.append(responsebody);
		try {
			OutputStream out = sock.getOutputStream();
			out.write(sb.toString().getBytes());
			out.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		elect.setState("NORMAL");
	}
	
	public void sendDataServerlist() {
		StringBuffer sb = new StringBuffer();
		String responsebody = elect.getDataServers().toJSONString();
		String responseheader = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responsebody.getBytes().length + "\n\n";
		sb.append(responseheader);
		sb.append(responsebody);
		try {
			OutputStream out = sock.getOutputStream();
			out.write(sb.toString().getBytes());
			out.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void execution(HTTPRequestLine httprequestLine) {
		
		if (httprequestLine.getParameters("host").equals("dataserver")) {
 			int priority = elect.getPriority();
			elect.addDataserver(host, priority);
//				elect.addDataserver(address);
			JSONObject obj = new JSONObject();
			obj.put("primary", elect.getPrimary());
			obj.put("priority", priority);
			sendMessage(obj.toJSONString());
		}else if (httprequestLine.getParameters("host").equals("frontend")) {
//			elect.addDataserver(address);
			JSONObject obj = new JSONObject();
			obj.put("primary", elect.getPrimary());
			sendMessage(obj.toJSONString());
		}
	}
		
	public void sendMessage(String message) {
		StringBuffer sb = new StringBuffer();
		String responsebody = message;
		String responseheader = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responsebody.getBytes().length + "\n\n";
		sb.append(responseheader);
		sb.append(responsebody);
		try {
			OutputStream out = sock.getOutputStream();
			out.write(sb.toString().getBytes());
			out.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void sendPrimary() {
		StringBuffer sb = new StringBuffer();
		String responsebody = elect.getPrimary();
		String responseheader = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responsebody.getBytes().length + "\n\n";
		sb.append(responseheader);
		sb.append(responsebody);
		try {
			OutputStream out = sock.getOutputStream();
			out.write(sb.toString().getBytes());
			out.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 
//	public void sendElection() {
//		ExecutorService executor = Executors.newFixedThreadPool(10);
//		JSONObject dataServers = elect.getDataServers();
//		System.out.println(dataServers.toJSONString());
//		for (Object key : dataServers.keySet()) {
//			String dataserver_address = (String)key;
//			int priority = Integer.parseInt(dataServers.get(key).toString());
//			String[] dataserverInfo = dataserver_address.split(":");
//			String host = dataserverInfo[0];
//			int port = Integer.parseInt(dataserverInfo[1]);
//			JSONObject ElectionMessage = new JSONObject();
//			ElectionMessage.put("dataServers", dataServers.toJSONString());
//			ElectionMessage.put("priority", priority);
////			System.out.println(host);
////			System.out.println(port);
//			executor.execute(new SendElectionInfo(host, port, ElectionMessage));
//		}
//		executor.shutdown();
//		return;
//	}
//
//	private class SendElectionInfo implements Runnable {
//		private String host;
//		private int port;
//		private JSONObject ElectionMessage;
//
//		SendElectionInfo(String host, int port, JSONObject ElectionMessage) {
//			this.host = host;
//			this.port = port;
//			this.ElectionMessage = ElectionMessage;
//		}
//		@Override
//		public void run() {
//			// TODO Auto-generated method stub
//			Socket socket;
//			BufferedReader in;
//			try {
//				socket = new Socket(host, port);
////				logger.info("frontend connecting to the dataserver!");
//				OutputStream os = socket.getOutputStream();
//				StringBuffer header = new StringBuffer();
//				String uri = "/dataserver/election";
//				header.append("POST " + uri + " " + "HTTP/1.1" + LINE);
//				header.append("Host: " + host + ":" + port + LINE);
//				header.append("Content-Length: " + ElectionMessage.toJSONString().getBytes().length + LINE);
//				header.append(LINE);
//				header.append(ElectionMessage.toJSONString());
//				os.write(header.toString().getBytes());
//				os.flush();
//				String line;
//				/*
//				 * We get message from DataServer
//				 */
//				String requestLine = "";
//				in = new BufferedReader(
//						new InputStreamReader(socket.getInputStream()));
//				while (!(line = in.readLine().trim()).equals("")) {
//					// System.out.println(line);
//					requestLine = requestLine + line + "\n";
//				}
//				System.out.println(requestLine);
//				socket.close();
//			} catch (UnknownHostException e1) {
//				System.out.println(host + Integer.toString(port) + " is not available");
//			} catch (IOException e) {
//				System.out.println(host + Integer.toString(port) + " is not available");
//			} 
//		}
//	} 
}
