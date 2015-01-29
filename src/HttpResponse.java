
public class HttpResponse {

	public final static String LINE = "\r\n";
	
	public static String[] NotFound404() {
		String[] responseArr = new String[2];
		responseArr[0] = "<html><body>Not found the data server!</body></html>" + LINE;
		responseArr[1] = "HTTP/1.1 404 Not Found!\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		return responseArr;
	}
	
	public static String[] BadRequest400(String body, String Info) {
		String[] responseArr = new String[2];
		responseArr[0] = body + LINE;
		responseArr[1] = "HTTP/1.1 400 " + Info + "!\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		return responseArr;
	}
	
	public static String[] ServerNotAvailable503() {
		String[] responseArr = new String[2];
		responseArr[0] = "<html><body>DataServer is not available!</body></html>" + LINE;
		responseArr[1] = "HTTP/1.1 503 Bad Server!\n" + "Content-Length: "
				+ responseArr[0].getBytes().length + "\n\n";
		return responseArr;
	}
}
