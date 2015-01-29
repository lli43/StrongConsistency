import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class Electing {

	private MultiReaderLock lock;
	// state can be BROKEN, NORMAL and ELECTING
	private volatile String state ;
	private String primary;
	private volatile JSONObject dataserver_list;
	private volatile int priority;
	
	Electing() {
		lock = new MultiReaderLock();
		state = "BROKEN";
		primary = "";
		priority = 0;
		dataserver_list = new JSONObject();
	}
	
	public void addDataserver(String address, int prio) {
		lock.lockWrite();
		dataserver_list.put(address, prio);
		lock.unlockWrite();
	}
	
	public JSONObject getDataServers() {
		JSONObject jobj = new JSONObject();
		lock.lockRead();
		jobj.putAll(dataserver_list);
		lock.unlockRead();
		return jobj;
	}

	public int getPriority() {
		lock.lockWrite();
		int p = priority;
		priority += 1;
		lock.unlockWrite();
		return p;
	}
	public String getPrimary() {
		lock.lockRead();
		String p = primary;
		lock.unlockRead();
		return p;
	}

	public void setPrimary(String primary) {
		lock.lockWrite();
		this.primary = primary;
		lock.unlockWrite();
	}

	public String getState() {
		lock.lockWrite();
		String s = state;
		lock.unlockWrite();
		return s;
	}

	public void setState(String state) {
		lock.lockRead();
		this.state = state;
		lock.unlockRead();
	}
}
