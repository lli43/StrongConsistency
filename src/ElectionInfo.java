
public class ElectionInfo {
	
	private int primaryID = -1;
	private String primary_host = null;
	private int primary_port = 0;
	private MultiReaderLock lock = new MultiReaderLock();
	
	private String disHOST;
	private int disPORT;
	private String host;
	private int port;
	
	public void setDis(String disHOST, int disPORT) {
		this.disHOST = disHOST;
		this.disPORT = disPORT;
	}
	
	public void setLocal(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	public String getLocalHost() {
		return host;
	}
	
	public int getLocalPort() {
		return port;
	}
	
	public String getDisHost() {
		return disHOST;
	}
	
	public int getDisPort() {
		return disPORT;
	}
	
	public int getPrimaryID() {
		
		return primaryID;
	}
	public void setPrimaryID(int primaryID) {
		this.primaryID = primaryID;
	}
	public String getPrimary_host() {
		lock.lockRead();
		String p = primary_host;
		lock.unlockRead();
		return p;
	}
	public void setPrimary_host(String primary_host) {
		lock.lockWrite();
		this.primary_host = primary_host;
		lock.unlockWrite();
	}
	public int getPrimary_port() {
		lock.lockRead();
		int port = primary_port;
		lock.unlockRead();
		return port;
	}
	public void setPrimary_port(int primary_port) {
		lock.lockWrite();
		this.primary_port = primary_port;
		lock.unlockWrite();
	}
}
