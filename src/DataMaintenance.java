import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

public class DataMaintenance {

	/**
	 * DataMaintenance has two parameters, one is cacheMap which has key value pairs. Key is a string, and value is
	 * a JSON object, format is like {"q": "searchterm", "v": versionnum, "tweets": ["tw1", "tw2"]}
	 * lock is used for multi-threads safety.
	 */
	private int totalVersion;
	private HashMap<String, JSONObject> dataMap;
	private MultiReaderLock lock;
	private Logger logger;
	private TreeMap<Integer, JSONObject> log;
	
	public DataMaintenance(Logger logger) {
		dataMap = new HashMap<String, JSONObject>();
		lock = new MultiReaderLock();
		this.logger = logger;
		totalVersion = 0;
		log = new TreeMap<Integer, JSONObject>();
	}
	
	public void setSnapShot(JSONObject jbody) {
		lock.lockWrite();
		totalVersion = Integer.parseInt(jbody.get("totalVersion").toString());
		JSONObject obj = (JSONObject)jbody.get("data");
		for (Object key : obj.keySet()) {
			dataMap.put((String)key, (JSONObject)obj.get(key));
		}
		lock.unlockWrite();
	}
	
	public void addLog(int timeStamp, JSONObject jobj) {
		log.put(timeStamp, jobj);
	}
	
	public JSONObject getDifferentLog(int diff) {
		JSONObject obj = new JSONObject();
		for (int i = diff - 1; i >= 0; i --) {
			obj.put(log.lastKey() - i, log.get(log.lastKey() - i));
		}
		System.out.println(obj.toJSONString() + "987654321");
		return obj;
	}
	
	public void updataDifferentdata(JSONObject differentData) {
		System.out.println(differentData.toJSONString() + "123456789");
		JSONObject obj = (JSONObject) differentData.get("data");
		for (Object timeStamp : obj.keySet()) {
			String key = (String)timeStamp;
			int k = Integer.parseInt(key);
			if (!log.containsKey(k)) {
				System.out.println(obj.toJSONString() + "123456789");
				JSONObject jobj = (JSONObject) obj.get(timeStamp);
				log.put(k, jobj);
				String tweet = jobj.get("tweet").toString();
				JSONArray keyArray = (JSONArray)jobj.get("hashtags");
				for (Object keyA : keyArray) {
					updateTweets((String)keyA, tweet);
				}
			}
		}
		totalVersion = Integer.parseInt(differentData.get("totalVersion").toString());
	}
	
	public String getSnapShot() {
		lock.lockRead();
		if (dataMap.isEmpty()) {
			lock.unlockRead();
			return null;
		}
		JSONObject totalobj = new JSONObject();
		JSONObject obj = new JSONObject();
		for (String key : dataMap.keySet()) {
			obj.put(key, dataMap.get(key));
		}
		totalobj.put("totalVersion", totalVersion);
		totalobj.put("data", obj);
		String content = totalobj.toJSONString();
		lock.unlockRead();
		return content;
	}
	/*
	 * We can get JSON object via specific key by getJSONByKey
	 */
	public String getJSONByKey(String key) {
		lock.lockRead();
		logger.debug("get JSON object: " + key);
		JSONObject JSONObj = dataMap.get(key);
		lock.unlockRead();
		return JSONObj.toJSONString();
	}
	/*
	 * update the total version number
	 */
	public void updatetotalVersion() {
		lock.lockWrite();
		totalVersion += 1;
		lock.unlockWrite();
	}
	/*
	 * get the total version number
	 */
	public int getTotalVersion() {
		int v;
		lock.lockRead();
		v = totalVersion;
		lock.unlockRead();
		return v;
	}
	
//	public void addLog(JSONObject obj) {
//		lock.lockWrite();
//		JSONObject jobj = new JSONObject();
//		jobj.put("log", obj);
//		jobj.put("totalVersion", totalVersion);
//		lock.unlockWrite();
//	}
//	
//	public ArrayList<JSONObject> getLog(int different) {
//		lock.lockRead();
//		ArrayList<JSONObject> logArray = new ArrayList<JSONObject>();
//		logArray.addAll(log.subList(log.size() - 1 - different, log.size() - 1));
//		lock.unlockRead();
//		return logArray;
//	}
//	
//	public void updataDifferentdata(JSONObject differentData) {
//		int newestVersionNum = Integer.parseInt(differentData.get("totalVersion").toString());
//		int localTotalVersion = getTotalVersion();
//		if (newestVersionNum == localTotalVersion) {
//			return;
//		}
//		int diff = newestVersionNum - localTotalVersion;
//		ArrayList<JSONObject> data = (ArrayList<JSONObject>)differentData.get("data");
//		ArrayList<JSONObject> dataDiff = new ArrayList<JSONObject>();
//		dataDiff.addAll((data).subList(data.size() - diff - 1, data.size() - 1));
//		for (JSONObject obj : dataDiff) {
//			String tweet = obj.get("tweet").toString();
//			JSONArray keyArray = (JSONArray)obj.get("hashtags");
//			for (Object key : keyArray) {
//				updateTweets((String)key, tweet);
//			}
//		}
//	}
	
	/*
	 * We can update tweetJSON object by getJSONByKey
	 */
	@SuppressWarnings("unchecked")
	public void updateTweets(String key, String tweetBody) {
		lock.lockWrite();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (dataMap.containsKey(key)) {
			logger.debug("The key: " + key + " is already in data maintenance, update it.");
			((JSONArray)dataMap.get(key).get("tweets")).add(tweetBody);
			int version = Integer.parseInt(dataMap.get(key).get("v").toString()) + 1;
			dataMap.get(key).put("v", version);
		} else {
			logger.debug("The key: " + key + " is not in data maintenance, create it.");
			JSONObject jsonObj = new JSONObject();
			JSONArray tweets = new JSONArray();
			tweets.add(tweetBody);
			jsonObj.put("q", key);
			jsonObj.put("v", 1);
			jsonObj.put("tweets", tweets);
			dataMap.put(key, jsonObj);
		}
		lock.unlockWrite();
	}
	/*
	 * We can get the version of the data server by key
	 */
	public int getVersion(String key) {
		
		int version;
		
		lock.lockRead();
		logger.debug("Get the key: " + key + "'s version");
		if (dataMap.containsKey(key)) {
			JSONObject JSONObj = dataMap.get(key);
			version = Integer.parseInt(JSONObj.get("v").toString()) ;
		} else {
			version = -1;
		}
		lock.unlockRead();
		return version;
	}
	/*
	 * Check if the JSON object associated with the key in the HASHMAP exists
	 */
	public boolean containSearchTerm(String key) {
		lock.lockRead();
		boolean bool = dataMap.containsKey(key);
		lock.unlockRead();
		return bool;
	}
}
