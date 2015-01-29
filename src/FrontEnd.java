import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class FrontEnd extends Thread {

	private final static String LINE = "\r\n";
	private final static String disHOST = "mc07";
	private final static int disPORT = 5030;
	private Logger logger = LogManager.getLogger(FrontEnd.class);
	private String HOST = "localhost";
	private int PORT = 5040;
	public ElectionInfo elec = new ElectionInfo();
	
	FrontEnd() {
		elec.setDis(disHOST, disPORT);
		elec.setLocal(HOST, PORT);
	}
	FrontEnd(String host, String port) {
		elec.setDis(disHOST, disPORT);
		elec.setLocal(host, Integer.parseInt(port));
		HOST = host;
		PORT = Integer.parseInt(port);
	}
	public static void main(String[] args) {
		FrontEnd frontEnd;
		if (args.length > 0) {
			frontEnd = new FrontEnd(args[0], args[1]);
		} else {
			frontEnd = new FrontEnd();
		}
		frontEnd.start();
//		JSONObject configurationObj = frontEnd.getDataServerAddr();
		try {
			frontEnd.serverStart();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			frontEnd.logger.error("Cannot open the FrontEnd!");
		}
	}

	public void serverStart() throws Exception {

//		int PORT = Integer.parseInt(configurationObj.get("port").toString());
		@SuppressWarnings("resource")
		ServerSocket serversock = new ServerSocket(PORT);
		
		logger.info("FrontEnd start!");
		Cache cache = new Cache(logger);
		ExecutorService executor = Executors.newFixedThreadPool(10);
//		WorkQueue workQueue = new WorkQueue(10);
		while (true) {
			Socket sock = serversock.accept();
			executor.execute(new RequestProcessor(sock, cache, logger, elec));
		}
	}
	
	/*
	 * When the front end server start, it need to get the primary from the discovery server.
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		logger.info("Get the primary from the discovery server");
		JSONObject jobj = new JSONObject();
		int i = 0;
		while (true) {
			i++;
			String uri = "/discovery/getPrimary?host=frontend";
			
			String sendBody = null;
			String body = sendAndGetMess("GET" , uri, sendBody, disHOST, disPORT);
			if (body.equals("")) {
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (i > 3)
					break;
				continue;
			}
			
			try {
				JSONParser jParser = new JSONParser();
				Object obj = jParser.parse(body);
				jobj = (JSONObject) obj;
				
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				break;
			}
			
			String[] primaryAddr = jobj.get("primary").toString().split(":");
			elec.setPrimary_host(primaryAddr[0]);
			elec.setPrimary_port(Integer.parseInt(primaryAddr[1]));
			break;
		}
		System.out.println(elec.getPrimary_host() + ":" + elec.getPrimary_port());
	}

	public String sendAndGetMess(String method, String uri, String sendBody, String disHost, int disPort) {
		logger.debug(HOST + ":" + PORT + " WITH " + disHost + ":" + disPort + " Connection start");
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
				System.out.println(line);
				requestLine = requestLine + line + "\n";
				if (line.startsWith("Content-Length")) {
					String[] l = line.split(":");
					content_Length = Integer.parseInt(l[1].trim());
				}
			}
			 System.out.println(requestLine);
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
				 System.out.println(body);
			}
			sock.close();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			logger.debug(HOST + ":" + PORT + " WITH " + disHost + ":" + disPort + " Connection is broken");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.debug(HOST + ":" + PORT + " WITH " + disHost + ":" + disPort + " Connection is broken");
		}
		return body;
	}
	/*
	 * Read the FEconfiguration from the FEConfiguration.txt, including frontend's 
	 * port number and dataserver's address. Store them into a JSON body.
	 */
//	public JSONObject getDataServerAddr() {
//		String fileName = "FEConfiguration.txt";
//		String content = "";
//		try {
//			content = parseFile(fileName);
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		JSONObject jobj = new JSONObject();
//		try {
//			JSONParser jParser = new JSONParser();
//			Object obj = jParser.parse(content);
//			jobj = (JSONObject)obj;
////			System.out.println(jobj.toJSONString());
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//		System.out.println(jobj.toJSONString());
//		return jobj;
//	}
//
//	public String parseFile(String fileName) throws IOException,
//			FileNotFoundException {
//
//		File file = new File(fileName);
//		FileReader filereader = new FileReader(file);
//		BufferedReader bufferedreader = new BufferedReader(filereader);
//		String tempString = null;
//		String content = "";
//		// read line by line
//		while ((tempString = bufferedreader.readLine()) != null) {
//			content = content + tempString;
//		}
//		bufferedreader.close();
//		return content;
//	}
}
