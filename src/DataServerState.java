import java.util.ArrayList;


public class DataServerState {
	private MultiReaderLock lock;
	// state can be BROKEN, NORMAL and ELECTING
	private MultiReaderLock lock1;
	private volatile String state ;
	private boolean isprimary;
	private volatile String primary;
	private volatile int primaryPriority;
	private ArrayList<String> seconderys;
	private String host;
	private int port;
	
	DataServerState() {
		seconderys = new ArrayList<String>();
		lock = new MultiReaderLock();
		lock1 = new MultiReaderLock();
		setState("NEW");
		setIsprimary(false);
	}

	public void setHost(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	public String getHost() {
		return host;
	}
	
	public int getPort() {
		return port;
	}
	
	public void addSecondery(String secondAdd) {
		seconderys.add(secondAdd);
	}
	
	public ArrayList<String> getAllseconderies() {
		ArrayList<String> s = new ArrayList<String>();
		s = (ArrayList<String>) seconderys.clone();
		return s;
	}
	
	public String getState() {
		lock.lockRead();
		String stat = state;
		lock.unlockRead();
		return stat;
	}

	public void setState(String state) {
		lock.lockWrite();
		this.state = state;
		lock.unlockWrite();
	}

	public boolean isIsprimary() {
		return isprimary;
	}

	public void setIsprimary(boolean isprimary) {
		this.isprimary = isprimary;
	}

	public String getPrimary() {
		lock1.lockRead();
		String p = primary;
		lock1.unlockRead();
		return p;
	}

	public void setPrimary(String primary) {
		lock1.lockWrite();
		this.primary = primary;
		lock1.unlockWrite();
	}

	public int getPrimaryPriority() {
		lock1.lockRead();
		int p = primaryPriority;
		lock1.unlockRead();
		return p;
	}

	public void setPrimaryPriority(int primaryPriority) {
		lock1.lockWrite();
		this.primaryPriority = primaryPriority;
		lock1.unlockWrite();
	}
}
