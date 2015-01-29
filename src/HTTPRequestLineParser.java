import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;


public class HTTPRequestLineParser {

	/**
	 * This method takes as input the Request-Line exactly as it is read from the socket.
	 * It returns a Java object of type HTTPRequestLine containing a Java representation of
	 * the line.
	 *
	 * The signature of this method may be modified to throw exceptions you feel are appropriate.
	 * The parameters and return type may not be modified.
	 *
	 * 
	 * @param line
	 * @return
	 */
	public static HTTPRequestLine parse(String line, Logger logger) {
	    //A Request-Line is a METHOD followed by SPACE followed by URI followed by SPACE followed by VERSION
	    //A VERSION is 'HTTP/' followed by 1.0 or 1.1
	    //A URI is a '/' followed by PATH followed by optional '?' PARAMS 
	    //PARAMS are of the form key'='value'&'

		// Once something is wrong, I will return null
		HTTPRequestLine httprequestLine = new HTTPRequestLine();

		HTTPConstants.HTTPMethod method;
		String uripath; 
		String httpVersion;
		HashMap<String, String> parameters = new HashMap<String, String>();
		
		
		// At first, split by "\n", we can get the request header.
		String[] lineArr = line.split("\n");
		/*
		 * get the first line of the request
		 */
		String firstLine = lineArr[0];
		// Then, we split the request header by " ", we can get some different parts of request header.
		String[] commArr = firstLine.split(" ");
		// Check if the number of parts in request header is 3. If the number is not 3, that is invalid request header.
		if (commArr.length != 3) {
//			System.out.println("The request's format is wrong!");
			logger.error("The request's format is wrong!");
			return null;
		}
		// I check if the method belong to the valid methods.
		try {
			method = HTTPConstants.HTTPMethod.valueOf(commArr[0]);
			httprequestLine.setMethod(method);
//			System.out.println(method.toString());
		} catch (IllegalArgumentException ex) {
//			System.out.println("No such method!");
			logger.error("No such method!");
			return null;
		}
		
		// Then, I check the URI format.
		if (commArr[1].equals("")) {
			return null;
		} else {
			String url = commArr[1];
			try {
				url = URLDecoder.decode(url, "utf-8");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				logger.error("The request's format is wrong!");
				return null;
			}
			
			/* Using regular expression is really convenient.
			 * \/([\w+\/\.])+(\?\w[\w ]*=\w[\w ]*(&\w[\w ]*=\w[\w ]*)*)?
			 */
			Pattern p = Pattern.compile("\\/([\\w+\\/\\.])+(\\?\\w[\\w ]*=\\w[\\w ]*(&\\w[\\w ]*=\\w[\\w ]*)*)?");
			Matcher m = p.matcher(url);
//			System.out.println(m.matches());
			if (!m.matches()) {
//				System.out.println("Wrong URI path!");
				logger.error("Wrong URI path!");
				return null;
			}
			
			
			// Then I split the URI by "?"
			String[] uris = url.split("\\?");
//			System.out.println(uris.length);
			
			if (uris.length == 2) {
				uripath = uris[0];
				// We store the URI's path.
				httprequestLine.setUripath(uripath);
				String[] parametersPairs = uris[1].split("&");
				httprequestLine.createParaHashM();
				// We store the parameters
				for (String paramaterPair : parametersPairs) {
					String[] pairs = paramaterPair.split("=");
					if (pairs.length != 2) {
//						System.out.println("Wrong parameters format!");
						logger.error("Wrong parameters format!");
						return null;
					}
					else {
//						System.out.println(pairs[0]+pairs[1]);
						// I check if the key already exists.
						if (parameters.containsKey(pairs[0])) {
//							System.out.println("Parameters cannot have same keys!");
							logger.error("Parameters cannot have same keys!");
							return null;
						}
						
						httprequestLine.addElement(pairs[0], pairs[1]);
					}
				}
				
			} else if (uris.length == 1) {
				uripath = uris[0];
				httprequestLine.setUripath(uripath);
				httprequestLine.setParameters(parameters);
			}
			else  {
//				System.out.println("The request's format is wrong!");
				logger.error("The request's format is wrong!");
				return null;
			}
		}
		
		
		// Check if the HTTP version is right.
		if (commArr[2].equals("")) {
			logger.error("Http Version is missing!");
//			System.out.println("Http Version is missing!");
			return null;
		} else {
			httpVersion = commArr[2];
			httprequestLine.setHttpversion(httpVersion);
			Pattern p = Pattern.compile("HTTP\\/1.(0|1)");
			Matcher m = p.matcher(httpVersion);
			if (!m.matches()) {
//				System.out.println("Http Version format is wrong!");
				logger.error("Http Version format is wrong!");
				return null;
			}
		}
		
		return httprequestLine;
	}
}
