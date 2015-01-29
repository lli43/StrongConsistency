import java.util.HashMap;

/**
HTTPRequestLine is a data structure that stores a Java representation of the parsed Request-Line.
 **/
public class HTTPRequestLine {

	private HTTPConstants.HTTPMethod method;
	private String uripath;
	private HashMap<String, String> parameters = null;
	private String httpversion;
	
	// Get method
	public HTTPConstants.HTTPMethod getMethod() {
		return method;
	}
	// Set method
	public void setMethod(HTTPConstants.HTTPMethod method) {
		this.method = method;
	}
	// Get URI path
	public String getUripath() {
		return uripath;
	}
	// Set URI path
	public void setUripath(String uripath) {
		this.uripath = uripath;
	}
	// Get value by specific key
	public String getParameters(String key) {
		if (parameters.containsKey(key))
			return parameters.get(key);
		return null;
	}
	// Check the hashmap is empty or not
	public boolean isParameterEmpty() {
		if (parameters.isEmpty())
			return true;
		return false;
	}
	// Get all parameters with key value pairs
	public String getParameters() {
		String result = "";
		if (isParameterEmpty()) {
			return result;
		} 
		for (String key : parameters.keySet()) {
			result = result + key + " = " + parameters.get(key) + "\n";
		}
		return result;
	}
	// Set hashMap 
	public void setParameters(HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	// Get HTTP version
	public String getHttpversion() {
		return httpversion;
	}
	// Set HTTP version
	public void setHttpversion(String httpversion) {
		this.httpversion = httpversion;
	}
	// Check if specific key exists in hashMap
	public boolean containsKey(String key) {
		if (parameters.containsKey(key))
			return true;
		return false;
	}
	public void createParaHashM() {
		parameters = new HashMap<String, String>();
	}
	public void addElement(String key, String value) {
		parameters.put(key, value);
	}
	
	public int getKeyNumber() {
		int keynumber = 0;
		if (parameters == null || parameters.isEmpty()) {
			return keynumber;
		}
		return parameters.size();
	}
	
}
