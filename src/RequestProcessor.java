import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URLEncoder;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class RequestProcessor implements Runnable {

	private Socket sock;

//	public String host = "localhost";
//	public int PORT = 5050;
	private String host = "";
	private int PORT;
	private final static String LINE = "\r\n";
	public Cache cache;
	private Logger logger;
	private ElectionInfo elec;
	public RequestProcessor(Socket sock, Cache cache, Logger logger, ElectionInfo elec) {
		this.sock = sock;
		this.cache = cache;
		this.logger = logger;
		this.elec = elec;
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
				// System.out.println(line);
				requestLine = requestLine + line + "\n";
				if (line.startsWith("Content-Length")) {
					String[] l = line.split(":");
					content_Length = Integer.parseInt(l[1].trim());
				}
			}
//			System.out.println(requestLine);
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
		String responsebody = "";
		String responseheaders = "";
		String[] responseArr = new String[2];
		if (httprequestLine == null) {
			responsebody = "<html><body>This is an invalid request!</body></html>" + LINE;
			responseheaders = "HTTP/1.1 400 Bad request!\n"
					+ "Content-Length: " + responsebody.getBytes().length
					+ "\n\n";
		} else {
			
			/*
			 * Check the method if it is a post or a get
			 * Execute get or post according to what method it is
			 */
			logger.debug("Check the method if it is a post or a get");
			if (httprequestLine.getMethod().toString().equals("POST")) {                      //POST
				responseArr = executePost(httprequestLine, body);
				responsebody = responseArr[0];
				responseheaders = responseArr[1];
			} else if (httprequestLine.getMethod().toString().equals("GET")) {                //GET
				responseArr = executeGet(httprequestLine);
				responsebody = responseArr[0];
				responseheaders = responseArr[1];
			} else {
				responsebody = "<html><body>Method Not Allowed!</body></html>" + LINE;
				responseheaders = "HTTP/1.1 405 Method Not Allowed!\n" + "Content-Length: "
						+ responsebody.getBytes().length + "\n\n";
			}
			
		}
		
		OutputStream out;
		try {
			out = sock.getOutputStream();
			out.write(responseheaders.getBytes());
			out.write(responsebody.getBytes());
			out.flush();
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("Client is closed when return to client!");
		}

		try {
			sock.close();
			logger.info("connection with client is over!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("Error when close the connection with client!");
		}
	}
	/*
	 * Check the post body's format. If format is valid, send it to DS.
	 */
	public String[] executePost(HTTPRequestLine httprequestLine, String body) {
		
		logger.info("start posting!");
		String[] responseArr = new String[2];
		/*
		 * Check the URI if it is "/tweets"
		 */
		logger.debug("check the post uri if it is /tweets");
		if (!httprequestLine.getUripath().equals("/tweets")) {
			logger.error("post uri is wrong!");
			return HttpResponse.NotFound404();
		}
		if (httprequestLine.getKeyNumber() != 0) {
			String httpbody = "<html><body>uri is Wrong!</body></html>";
			String info = "Wrong Format";
			logger.error("uri is wrong!");
			return HttpResponse.BadRequest400(httpbody, info);
		}
		/*
		 * Check the body if it is JSON OBJ
		 */
		JSONObject jobj = new JSONObject();
		try {
			JSONParser jParser = new JSONParser();
			Object obj = jParser.parse(body);
			jobj = (JSONObject)obj;
//			System.out.println(jobj.toJSONString());
		}
		
		/*
		 * If it is not a JSON, return 400
		 */
		catch (ParseException e) {
			// TODO Auto-generated catch block
			String httpbody = "<html><body>Bad post body!</body></html>";
			String info = "Wrong Format";
			logger.error("post body is wrong!");
			return HttpResponse.BadRequest400(httpbody, info);
		}
		/*
		 * Check the post body if it is like {"text":"XXXX"}
		 */
		if (!jobj.containsKey("text")) {
			String httpbody = "<html><body>Bad post body!</body></html>";
			String info = "Wrong Format";
			logger.error("post body is wrong!");
			return HttpResponse.BadRequest400(httpbody, info);
		}
		/*
		 * Check if there is a single '#', if exists, return 400
		 */
		String textBody = jobj.get("text").toString();
//		System.out.println(textBody);
		String regForInvalid1 = "\\s#\\s";
		String regForInvalid2 = ".*\\s#";
		Pattern pForInvalid1 = Pattern.compile(regForInvalid1);
		Pattern pForInvalid2 = Pattern.compile(regForInvalid2);
		Matcher mForInvalid1 = pForInvalid1.matcher(textBody);
		Matcher mForInvalid2 = pForInvalid2.matcher(textBody);
		if (mForInvalid1.find() || mForInvalid2.matches()) {
			String httpbody = "<html><body>Bad post body!</body></html>";
			String info = "Wrong Format";
			logger.error("post body is wrong!");
			return HttpResponse.BadRequest400(httpbody, info);
		}
		/*
		 * Get the #keys and store them into list
		 */
		JSONArray keyArray = new JSONArray();
		String regForvalid = "(#([^\\s]+))";
		Pattern pForvalid = Pattern.compile(regForvalid);
		Matcher mForvalid = pForvalid.matcher(textBody);
		while (mForvalid.find()) {
//			System.out.println("========" + mForvalid.group(2));
			keyArray.add(mForvalid.group(2));
		}
		
		if (keyArray.isEmpty()) {
			String httpbody = "<html><body>Bad post body!</body></html>";
			String info = "Wrong Format";
			logger.error("post body is wrong!");
			return HttpResponse.BadRequest400(httpbody, info);
		}
		/*
		 * Create a JSON object to send to DS
		 */
		JSONObject postJSONObject = new JSONObject();
		postJSONObject.put("tweet", textBody);
		postJSONObject.put("hashtags", keyArray);
//		System.out.println(postJSONObject.toJSONString());
		/*
		 * doPost use socket connect with DataServer
		 */
		responseArr = doPost(postJSONObject, httprequestLine);
		return responseArr;
	}
	
	public String[] executeGet(HTTPRequestLine httprequestLine) {
		String[] responseArr = new String[2];
		/*
		 * Check the URI if it is "/tweets"
		 */
		logger.debug("check the get uri if it is /tweets");
		if (!httprequestLine.getUripath().equals("/tweets")) {
			logger.error("get uri is wrong!");
			return HttpResponse.NotFound404();
		}
		/*
		 * Check searchterm format, if it is valid, get it.
		 */
		logger.debug("check the get searchterm");
		String searchterm = "";
		if (httprequestLine.getKeyNumber() != 1) {
			String httpbody = "<html><body>Searchterm is Wrong!</body></html>";
			String info = "Wrong Format";
			logger.error("searchterm is wrong!");
			return HttpResponse.BadRequest400(httpbody, info);
		}
		if (!httprequestLine.containsKey("q")) {
			String httpbody = "<html><body>Searchterm is Wrong!</body></html>";
			String info = "Wrong Format";
			logger.error("searchterm is wrong!");
			return HttpResponse.BadRequest400(httpbody, info);
		} else {
			searchterm = httprequestLine.getParameters("q");
		}
		responseArr = doGet(searchterm, httprequestLine);
		return responseArr;
	}

	/*
	 * if the old primary is not available, the frontend will go to discovery to
	 * get the new primary, if fail 3 times, the frontend will return client that
	 * the data server is unavailable.
	 */
	public void getPrimaryDS() {
		logger.debug("Get the new primary.");
		String method = "GET";
		String uri = "/discovery/getPrimary?host=frontend";
		String httpversion = "HTTP/1.1";
		try {
			uri = URLEncoder.encode(uri, "utf-8");
		} catch (UnsupportedEncodingException e2) {
			e2.getMessage();
		}
		StringBuffer header = new StringBuffer();
		header.append(method + " " + uri + " " + httpversion + LINE);
		header.append("Host: " + elec.getLocalHost() + ":" + elec.getLocalPort() + LINE);
		header.append(LINE);
		String requestLine = "";
		int content_Length = 0;
		String body = null;
		try {
			Socket sock = new Socket(elec.getDisHost(), elec.getDisPort());
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
			System.out.println("Connection is broken");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Connection is broken");
		}
		JSONObject jobj = new JSONObject();
		try {
			JSONParser jParser = new JSONParser();
			Object obj = jParser.parse(body);
			jobj = (JSONObject) obj;
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String[] primaryAddr = jobj.get("primary").toString().split(":");
		elec.setPrimary_host(primaryAddr[0]);
		elec.setPrimary_port(Integer.parseInt(primaryAddr[1]));
	}
	
	public String[] doPost(JSONObject JObj, HTTPRequestLine httprequestLine) {
		
		String[] responseArr = new String[2];
		int i = 0;
		while(true) {
			host = elec.getPrimary_host();
			PORT = elec.getPrimary_port();
			i ++;
			Socket socket;
			
			BufferedReader in;
			String requestLine = "";
			String body = "";
			int content_Length = 0;
			try {
				/*
				 * We open the socket connecting with the DataServer.
				 */
				socket = new Socket(host, PORT);
				logger.info("frontend connecting to the dataserver!");
				/*
				 * We write the HTTP post request and send the JSON body to DataServer
				 */
				OutputStream os = socket.getOutputStream();
				StringBuffer header = new StringBuffer();
				logger.info("frontend send post request to the dataserver!");
				header.append("POST " + httprequestLine.getUripath() + " " + httprequestLine.getHttpversion() + LINE);
				
				header.append("Host: " + host + ":" + PORT + LINE);
				header.append("Content-Length: " + JObj.toJSONString().getBytes().length + LINE);
				header.append("Content-Type: application/x-www-form-urlencoded" + LINE);
				header.append(LINE);
				header.append(JObj.toJSONString());
				os.write(header.toString().getBytes());
				os.flush();
				String line;
				/*
				 * We get message from DataServer
				 */
				
				in = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));
				while (!(line = in.readLine().trim()).equals("")) {
					// System.out.println(line);
					requestLine = requestLine + line + "\n";
					if (line.startsWith("Content-Length")) {
						String[] l = line.split(":");
						content_Length = Integer.parseInt(l[1].trim());
					}
				}
	//			System.out.println(requestLine);
				if (in.ready()) {
					char[] h = new char[content_Length];
					in.read(h);
					StringBuffer sb = new StringBuffer();
					sb.append(h);
					body = sb.toString().trim();
	//				System.out.println(body);
				}
				logger.info("FrontEnd already got result from dataserver");
				socket.close();
			}
			catch (UnknownHostException e1) {
				cache.clearMap();
				logger.error("The connection to Dataserver is closed!");
				
				if (i > 3) {
					return HttpResponse.ServerNotAvailable503();
				}
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				getPrimaryDS();
				continue;
			} catch (IOException e) {
				cache.clearMap();
				logger.error("The connection to Dataserver is closed!");
				
				if (i > 3) {
					return HttpResponse.ServerNotAvailable503();
				}
				try {
					Thread.sleep(200);
				} catch (InterruptedException ex) {
					// TODO Auto-generated catch block
					ex.printStackTrace();
				}
				getPrimaryDS();
				continue;
			}
			/*
			 * After sending post, I wait for the result from DataServer
			 */
			
			if (requestLine.startsWith("HTTP/1.1 201 Created")) {
				responseArr[0] = "<html><body>Created!</body></html>" + LINE;
				responseArr[1] = "HTTP/1.1 201 Created\n" + "Content-Length: "
						+ responseArr[0].getBytes().length + "\n\n";
				logger.info("Post body has already created!");
				return responseArr;
			} else {
				responseArr = HttpResponse.NotFound404();
				return responseArr;
			}
		}
	}
	/*
	 * Once we get the SearchTerm, we check the cache first, get the version, then send the JSON body
	 * to DataServer. If we get the JSON body, that mean we need to update our cache. Then return the 
	 * data back to clients.
	 */
	public String[] doGet(String searchterm, HTTPRequestLine httprequestLine) {
		
		int i = 0;
		while (true) {
			host = elec.getPrimary_host();
			PORT = elec.getPrimary_port();
			i++;
			String[] responseArr = new String[2];
			int version = cache.getVersion(searchterm);
	//		System.out.println(version);
			Socket socket;
			/*
			 * We write the HTTP request header
			 */
			String uri = httprequestLine.getUripath() + "?q=" + searchterm + "&v=" + version;
			try {
				uri = URLEncoder.encode(uri, "utf-8");
			} catch (UnsupportedEncodingException e2) {
				// TODO Auto-generated catch block
				String httpbody = "<html><body>Bad GET body!</body></html>";
				String info = "Wrong Format";
				return HttpResponse.BadRequest400(httpbody, info);
			}
			
			StringBuffer header = new StringBuffer();
			header.append("GET " + uri + " " + httprequestLine.getHttpversion() + LINE);
			header.append("Host: " + host + ":" + PORT + LINE);
			header.append(LINE);
			try {
				logger.info("Open socket connection with DataServer in get process.");
				socket = new Socket(host, PORT);
				/*
				 * Send HTTP request header
				 */
				OutputStream os = socket.getOutputStream();
				os.write(header.toString().getBytes());
				os.flush();
			} catch (UnknownHostException e1) {
				logger.error("The connection to Dataserver is closed!");
				if (i > 3)
					return HttpResponse.ServerNotAvailable503();
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				getPrimaryDS();
				continue;
			} catch (IOException e1) {
				
				logger.error("The connection to Dataserver is closed!");
				if (i > 3)
					return HttpResponse.ServerNotAvailable503();
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				getPrimaryDS();
				continue;
			}
			
			/*
			 * We get the JSON body which I want (Maybe not, because if the version 
			 * number is the same, we get not modified, and return the data which in
			 * cache)
			 */
			BufferedReader in;
			String requestLine = "";
			String body = "";
			String line;
			int content_Length = 0;
			try {
				in = new BufferedReader(
						new InputStreamReader(socket.getInputStream()));
				while (!(line = in.readLine().trim()).equals("")) {
					// System.out.println(line);
					requestLine = requestLine + line + "\n";
					if (line.startsWith("Content-Length")) {
						String[] l = line.split(":");
						content_Length = Integer.parseInt(l[1].trim());
					}
				}
	//			System.out.println(requestLine);
				if (in.ready()) {
					char[] h = new char[content_Length];
					in.read(h);
					StringBuffer sb = new StringBuffer();
					sb.append(h);
					body = sb.toString().trim();
	//				System.out.println(body);
				}
				logger.info("already get search body!");
				socket.close();
			} catch (IOException e) {
				
				logger.error("The connection to Dataserver is closed!");
				if (i > 3)
					return HttpResponse.ServerNotAvailable503();
				try {
					Thread.sleep(200);
				} catch (InterruptedException ex) {
					// TODO Auto-generated catch block
					ex.printStackTrace();
				}
				getPrimaryDS();
				continue;
			}
			/*
			 * We check the result which is from DataServer, if it is 200 OK, we return the JSON
			 * from DataServer, if it is 304 not modified, we return the data from cache.
			 */
			if (requestLine.startsWith("HTTP/1.1 200 OK!")) {
				
				JSONObject jobj = new JSONObject();
				try {
					JSONParser jParser = new JSONParser();
					Object obj = jParser.parse(body);
					jobj = (JSONObject)obj;
	//				System.out.println(jobj.toJSONString());
				}
				/*
				 * If it is not a JSON, return 400
				 */
				catch (ParseException e) {
					// TODO Auto-generated catch block
					String httpbody = "<html><body>Bad GET body!</body></html>";
					String info = "Wrong Format";
					return HttpResponse.BadRequest400(httpbody, info);
				}
				if (Integer.parseInt(jobj.get("v").toString()) != 0) {
					logger.info("update the cache!");
					cache.updateJSON(searchterm, jobj);
				}
	//			System.out.println("==========="+jobj.get("v"));
				JSONObject result = new JSONObject();
				result.putAll(jobj);
				result.remove("v");
				responseArr[0] = result.toJSONString() + LINE;
				responseArr[1] = "HTTP/1.1 200 OK!\n" + "Content-Length: "
						+ responseArr[0].getBytes().length + "\n\n";
				logger.info("return the updated json body!");
				System.out.println("I'm 200");
			} else if (requestLine.startsWith("HTTP/1.1 304")) {
				responseArr[0] = cache.getJSONByKey(searchterm) + LINE;
				responseArr[1] = "HTTP/1.1 200 Not Modified!\n" + "Content-Length: "
						+ responseArr[0].getBytes().length + "\n\n";
				logger.info("return the cache json body!");
				System.out.println("I'm 304");
	//			System.out.println(responseArr[0]);
			}
			return responseArr;
		}
	}
}
