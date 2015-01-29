import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class DSRequestProcessor implements Runnable  {

	private Socket sock;
	private DataMaintenance dataM;
	private final static String LINE = "\r\n";
	private Logger logger;
	private DataServerState dataSS;
	private int pending = 0;
	private MultiReaderLock lock = new MultiReaderLock();
	public DSRequestProcessor(Socket sock, DataMaintenance dataM, DataServerState dataSS, Logger logger) {
		this.sock = sock;
		this.dataM = dataM;
		this.logger = logger;
		this.dataSS = dataSS;
	}

	/*
	 * Parse the httprequestLine, store the method, URI and parameters into HTTPRequestLine
	 */
	HTTPRequestLine httprequestLine = new HTTPRequestLine();
	@Override
	public void run() {
		// TODO Auto-generated method stub
		BufferedReader in;
		String requestLine = "";
		String body = "";
		String target = "";
		int content_Length = 0;
		try {
			in = new BufferedReader(
					new InputStreamReader(sock.getInputStream()));
			String line;
			while (!(line = in.readLine().trim()).equals("")) {
				// System.out.println(line);
				requestLine = requestLine + line + "\n";
				if (line.startsWith("Content-Length")) {
					String[] l = line.split(":");
					content_Length = Integer.parseInt(l[1].trim());
				}
				if (line.startsWith("Host: ")) {
					String[] tar = line.split(" ");
					target = tar[1];
				}
			}
//			System.out.println(requestLine);

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
			logger.info("Got Json body from FrontEnd!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("FrontEnd is closed!");
			return;
		}
		httprequestLine = HTTPRequestLineParser.parse(requestLine, logger);
		String responseheaders = "";
		String responsebody = "";
		String[] responseArr = new String[2];
		
		if (httprequestLine == null) {
			responsebody = "<html><body>This is an invalid request!</body></html>";
			responseheaders = "HTTP/1.1 400 Bad request!\n"
					+ "Content-Length: " + responsebody.getBytes().length
					+ "\n\n";
		} else {
			/*
			 * Check the request method, it is GET or POST. And according to the different uri
			 * to do the execution
			 */
			logger.debug("Check the request from FrontEnd is get or post" + httprequestLine.getUripath());
			if (httprequestLine.getMethod().toString().equals("POST")) {
				
				// If it is POST, we parse the post body, then store it to DataMaintenance
				if (httprequestLine.getUripath().startsWith("/tweets")) {
					responseArr = updateDataM(body);
					responseheaders = responseArr[1];
					responsebody = responseArr[0];
				} else if (httprequestLine.getUripath().startsWith("/secondery/snapshot")) {
					responseArr = executeSnapShot(httprequestLine, body); 
					responseheaders = responseArr[1];
					responsebody = responseArr[0];
				} else if (httprequestLine.getUripath().startsWith("/seconderies/updataDiff")) {
					responseArr = updateDiff(body); 
					responseheaders = responseArr[1];
					responsebody = responseArr[0];
				}
			}
			else if (httprequestLine.getMethod().toString().equals("GET")) {
				if (httprequestLine.getUripath().startsWith("/tweets")) {
					responseArr = getData(httprequestLine);
					responseheaders = responseArr[1];
					responsebody = responseArr[0];
				} else if (httprequestLine.getUripath().startsWith("/dataserver/areualive")) {
					responseArr = getAreUAlive(httprequestLine);
					responseheaders = responseArr[1];
					responsebody = responseArr[0];
				} else if (httprequestLine.getUripath().startsWith("/primary/detection")) {
					responseArr = responseDetection(httprequestLine);
					responseheaders = responseArr[1];
					responsebody = responseArr[0];
				} else if (httprequestLine.getUripath().startsWith("/dataserver/Iamprimary")) {
					responseArr = setPrimary(httprequestLine, body); 
					responseheaders = responseArr[1];
					responsebody = responseArr[0];
				} else if (httprequestLine.getUripath().startsWith("/primary/addSecondery")) {
					returnBack();
					sendSnapShot(httprequestLine, body);
					dataSS.setState("NORMAL");
					return;
				} else if (httprequestLine.getUripath().startsWith("/seconderies/getTotalVersion")) {
					responseArr = getVersion(); 
					responseheaders = responseArr[1];
					responsebody = responseArr[0];
				} else if (httprequestLine.getUripath().startsWith("/seconderies/getNewest")) {
					responseArr = getNewest(body); 
					responseheaders = responseArr[1];
					responsebody = responseArr[0];
				}
				
			} else {
				responsebody = "<html><body>Method Not Allowed!</body></html>" + LINE;
				responseheaders = "HTTP/1.1 405 Method Not Allowed!\n" + "Content-Length: "
						+ responsebody.getBytes().length + "\n\n";
			}
		}
		
		OutputStream out;
		try {
			out = sock.getOutputStream();
			out.write((responseheaders + responsebody).getBytes());
//			out.write(responsebody.getBytes());
			logger.info("return the result to server");
			out.flush();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("server is closed!");
			return;
		}

		try {
			sock.close();
			logger.info("Connection with FrontEnd is over, waiting for next one!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("server is closed!");
			return;
		}
		
	}
	
	public String[] updateDiff(String body) {
		String[] responseArr = new String[2];
		JSONObject jobj = new JSONObject();
		try {
			JSONParser jParser = new JSONParser();
			Object obj = jParser.parse(body);
			jobj = (JSONObject)obj;
		}
		catch (ParseException e) {
			// TODO Auto-generated catch block
			logger.error("The post body from FrontEnd is wrong!");
		}
		dataM.updataDifferentdata(jobj);
		System.out.println(dataSS.getPort() + "++++++" + jobj.toJSONString());
		responseArr[0] = "OK";
		responseArr[1] = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		return responseArr;
	}
	
	public String[] getNewest(String body) {
		String[] responseArr = new String[2];
		JSONObject jobj = new JSONObject();
		try {
			JSONParser jParser = new JSONParser();
			Object obj = jParser.parse(body);
			jobj = (JSONObject)obj;
		}
		catch (ParseException e) {
			// TODO Auto-generated catch block
			logger.error("The post body from FrontEnd is wrong!");
		}
		int diff = Integer.parseInt(jobj.get("different").toString());
		JSONObject diffObj = new JSONObject();
		diffObj.put("data", dataM.getDifferentLog(diff));
		diffObj.put("totalVersion", dataM.getTotalVersion());
		System.out.println(dataSS.getPort() + "++++++" + diffObj.toJSONString());
		responseArr[0] = diffObj.toJSONString();
		responseArr[1] = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		return responseArr;
	}
	/*
	 * Get the total version from the DataMaintenance
	 */
	public String[] getVersion() {
		String[] responseArr = new String[2];
		JSONObject obj = new JSONObject();
		obj.put("totalVersion", dataM.getTotalVersion());
		responseArr[0] = obj.toJSONString();
		responseArr[1] = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		return responseArr;
	}
	
	public void returnBack() {
		OutputStream out;
		String responsebody = "OK";
		String responseheaders = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responsebody.getBytes().length + "\n\n";
		try {
			out = sock.getOutputStream();
			out.write((responseheaders + responsebody).getBytes());
//			out.write(responsebody.getBytes());
			logger.info("return the result to FrontEnd");
			out.flush();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("FrontEnd is closed!");
			return;
		}

		try {
			sock.close();
			logger.info("Connection with FrontEnd is over, waiting for next one!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("FrontEnd is closed!");
			return;
		}
	}
	
	/*
	 * When receive the snapshot data, it executeSnapShot
	 */
	public String[] executeSnapShot(HTTPRequestLine httprequestLine, String body) {
		logger.info(dataSS.getHost() + ":" + dataSS.getPort() + "executeSnapShot");
		String[] responseArr = new String[2];
		responseArr[0] = "OK";
		responseArr[1] = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		if (body.equals("")) {
			System.out.println("==============" + dataSS.getState());
			dataSS.setState("NORMAL");
			return responseArr;
		}
		JSONObject jobj = new JSONObject();
		try {
			JSONParser jParser = new JSONParser();
			Object obj = jParser.parse(body);
			jobj = (JSONObject)obj;
		}
		
		/*
		 * If it is not a JSON, return 400
		 */
		catch (ParseException e) {
			// TODO Auto-generated catch block
			String httpbody = "<html><body>Bad post body!</body></html>";
			String info = "Wrong Format";
			logger.error("The post body from FrontEnd is wrong!");
			return HttpResponse.BadRequest400(httpbody, info);
		}
		System.out.println(jobj.toJSONString());
		logger.info(dataSS.getHost() + ":" + dataSS.getPort() + "executeSnapShot" + jobj.toJSONString());
		dataM.setSnapShot(jobj);
		dataSS.setState("NORMAL");
		System.out.println(dataSS.getState());
		return responseArr;
	}
	
	/*
	 * primary send the snapshot data to the server which need.
	 */
	public void sendSnapShot(HTTPRequestLine httprequestLine, String body) {
		logger.info(dataSS.getHost() + ":" + dataSS.getPort() + "sendSnapShot");
		String[] target = body.split(":");
		System.out.println(target[1]);
		String content = dataM.getSnapShot();
		StringBuffer header = new StringBuffer();
		String uri = "/secondery/snapshot";
		String httpversion = "HTTP/1.1";
		header.append("POST" + " " + uri + " " + httpversion + LINE);
		header.append("Host: " + dataSS.getHost() + ":" + dataSS.getPort() + LINE);
		if (content != null)
			header.append("Content-Length : " + content.getBytes().length + LINE);
		
		header.append(LINE);
		if (content != null)
			header.append(content);
		
		try {
			Socket sock = new Socket(target[0], Integer.parseInt(target[1]));
			OutputStream out = sock.getOutputStream();
			out.write(header.toString().getBytes());
//			out.write(responsebody.getBytes());
			logger.info("return the result to FrontEnd");
			out.flush();
			
//			BufferedReader in;
//			String requestLine = "";
//			String getbody = "";
//			int content_Length = 0;
//			in = new BufferedReader(
//					new InputStreamReader(sock.getInputStream()));
//			String line;
//			while (!(line = in.readLine().trim()).equals("")) {
//				// System.out.println(line);
//				requestLine = requestLine + line + "\n";
//				if (line.startsWith("Content-Length")) {
//					String[] l = line.split(":");
//					content_Length = Integer.parseInt(l[1].trim());
//				}
//			}
//			System.out.println(requestLine);
//
//			/*
//			 * Get POST body if exists
//			 */
//			if (in.ready()) {
//				char[] h = new char[content_Length];
//				in.read(h);
//				StringBuffer sb = new StringBuffer();
//				sb.append(h);
//				getbody = sb.toString().trim();
//				System.out.println(getbody);
//			}
//			logger.info("Got Json body from FrontEnd!");
			sock.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("FrontEnd is closed!");
			return;
		}
		addToSecondery(httprequestLine, body);
	}
	
	public void addToSecondery(HTTPRequestLine httprequestLine, String body) {
		if (!dataSS.getAllseconderies().contains(body))
			dataSS.addSecondery(body);
	}
	
	/*
	 * After the secondery getting the primary info, it store the primary
	 */
	public String[] setPrimary(HTTPRequestLine httprequestLine, String body) {
		String[] responseArr = new String[2];
		JSONObject jobj = new JSONObject();
		try {
			JSONParser jParser = new JSONParser();
			Object obj = jParser.parse(body);
			jobj = (JSONObject)obj;
		}
		/*
		 * If it is not a JSON, return 400
		 */
		catch (ParseException e) {
			// TODO Auto-generated catch block
			String httpbody = "<html><body>Bad post body!</body></html>";
			String info = "Wrong Format";
			logger.error("The post body from FrontEnd is wrong!");
			return HttpResponse.BadRequest400(httpbody, info);
		}
		while (!dataSS.getState().equals("WAITING")) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		dataSS.setPrimary(jobj.get("primary").toString());
		logger.info(dataSS.getHost() + ":" + dataSS.getPort() + jobj.get(jobj.get("primary").toString()));
		responseArr[0] = "OK";
		responseArr[1] = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		return responseArr;
	}
	
	/*
	 * response the detection
	 */
	public String[] responseDetection(HTTPRequestLine httprequestLine) {
		String[] responseArr = new String[2];
		responseArr[0] = "YES";
		responseArr[1] = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		return responseArr;
	}
	
	public String[] getAreUAlive(HTTPRequestLine httprequestLine) {
		String[] responseArr = new String[2];
		responseArr[0] = "YES";
		responseArr[1] = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		return responseArr;
	}
	
	public String[] getData(HTTPRequestLine httprequestLine) {
		String[] responseArr = new String[2];
		/*
		 * Check if the parameters exist
		 */
		String parameterQ;
		int parameterV;
		try {
			parameterQ = httprequestLine.getParameters("q");
			parameterV = Integer.parseInt(httprequestLine.getParameters("v"));
		} catch (Exception ex) {
			// TODO Auto-generated catch block
			String httpbody = "<html><body>Bad GET body!</body></html>";
			String info = "Wrong Format";
			logger.error("Get format is wrong!");
			return HttpResponse.BadRequest400(httpbody, info);
		}
		/*
		 * First we check the version number 
		 */
//		System.out.println("==========="+parameterV);
//		System.out.println("==========="+dataM.getVersion(parameterQ));
		logger.debug("Check the version number!");
		if (parameterV == dataM.getVersion(parameterQ)) {
			responseArr[0] = "<html><body>Not Modified!</body></html>";
			responseArr[1] = "HTTP/1.1 304 Not Modified!\n" + "Content-Length: "
					+ responseArr[0].getBytes().length + "\n\n";
			return responseArr;
		}
		/*
		 * If version number is not the same, we get the new data
		 * But we check if the key exists, if not, we return the 
		 * {"q": "searchterm", "v": versionnum, "tweets": []}
		 */
		if (!dataM.containSearchTerm(parameterQ)) {
			logger.info("Return the empty json body to FrontEnd!");
			JSONObject dataObj = new JSONObject();
			JSONArray tweetArr = new JSONArray();
			dataObj.put("q", parameterQ);
			dataObj.put("v", 0);
			dataObj.put("tweets", tweetArr);
			responseArr[0] = dataObj.toJSONString() + LINE;
			responseArr[1] = "HTTP/1.1 200 OK!\n" + "Content-Length: "
					+ responseArr[0].getBytes().length + "\n\n";
			return responseArr;
		} 
		logger.info("Return the json body to FrontEnd!");
		responseArr[0] = dataM.getJSONByKey(parameterQ) + LINE;
		responseArr[1] = "HTTP/1.1 200 OK!\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		return responseArr;
	}
	
	public String[] updateDataM(String postBody) {
//		System.out.println(postBody + "+++++++++++++++++");
		
		/*
		 * Change the postBody from STRING TO JSON
		 */
		
		String[] responseArr = new String[2];
		JSONObject jobj = new JSONObject();
		try {
			JSONParser jParser = new JSONParser();
			Object obj = jParser.parse(postBody);
			jobj = (JSONObject)obj;
		}
		/*
		 * If it is not a JSON, return 400
		 */
		catch (ParseException e) {
			// TODO Auto-generated catch block
			String httpbody = "<html><body>Bad post body!</body></html>";
			String info = "Wrong Format";
			logger.error("The post body from FrontEnd is wrong!");
			return HttpResponse.BadRequest400(httpbody, info);
		}
		dataM.updatetotalVersion();
		if (dataSS.isIsprimary()) {
			logger.info(dataSS.getHost() + ":" + dataSS.getPort() + "I'm primary, broadcast the data to all seconderies");
			
			int timeStamp = dataM.getTotalVersion();
			dataM.addLog(timeStamp, jobj);
			jobj.put("timeStamp", timeStamp);
			System.out.println("ceshiceshiceshiceshi11111111111111");
			broadCastPost(jobj.toJSONString());
//		System.out.println("ceshiceshiceshiceshi");
		} else {
			logger.info(dataSS.getHost() + ":" + dataSS.getPort() + "I'm secondery, store this data");
			int timeS = Integer.parseInt(jobj.get("timeStamp").toString());
			jobj.remove("timeStamp");
			System.out.println(jobj.toJSONString());
			dataM.addLog(timeS, jobj);
		}
		/*
		 * Update the Tweet to the DataMaintenance
		 */
		String tweet = jobj.get("tweet").toString();
		JSONArray keyArray = (JSONArray)jobj.get("hashtags");
//		System.out.println(tweet);
		
		for (int i = 0; i < keyArray.size(); i++) {
			dataM.updateTweets(keyArray.get(i).toString(), tweet);
		}
		
		logger.info("The post body is added to dataServer!");
		/*
		 * If update successfully, return 201 created.
		 */
		
		responseArr[0] = "<html><body>Created!</body></html>" + LINE;
		responseArr[1] = "HTTP/1.1 201 Created\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		
		return responseArr;
	}

	public void broadCastPost(String sendBody) {
//		System.out.println(sendBody + "==========" + dataSS.getAllseconderies().size());
		String method = "POST";
		String uri = "/tweets";
		ArrayList<String> seconderies = dataSS.getAllseconderies();
//		ExecutorService executor = Executors.newFixedThreadPool(20);
		for (String host : seconderies) {
			System.out.println(host + "ajdsfkljalsdfajdf");
			String[] secAddr = host.split(":");
			sendAndGetMess(method, uri, sendBody, secAddr[0], Integer.parseInt(secAddr[1]));
//			executor.execute(new BroadCast(method, uri, sendBody, secAddr[0], Integer.parseInt(secAddr[1])));
		}
//		executor.shutdown();
	}
	
	public class BroadCast implements Runnable {

		private String uri;
		private String method;
		private String sendBody;
		private String host;
		private int port;
		BroadCast(String method, String uri, String sendBody, String host, int port) {
			this.method = method;
			this.uri = uri;
			this.sendBody = sendBody;
			this.host = host;
			this.port = port;
			incrementPending();
		}
		@Override
		public void run() {
			// TODO Auto-generated method stub
			String response = sendAndGetMess(method, uri, sendBody, host, port);
			decrementPending();
		}
	}
	
	public String sendAndGetMess(String method, String uri, String sendBody, String host, int port) {
		String httpversion = "HTTP/1.1";
		try {
			uri = URLEncoder.encode(uri, "utf-8");
		} catch (UnsupportedEncodingException e2) {
			e2.getMessage();
		}
		StringBuffer header = new StringBuffer();
		header.append(method + " " + uri + " " + httpversion + LINE);
		header.append("Host: " + dataSS.getHost() + ":" + dataSS.getPort() + LINE);
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
			Socket sock = new Socket(host, port);
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
			System.out.println(host + ":" + port + "Connection is broken");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(host + ":" + port + "Connection is broken");
		}
		return body;
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
