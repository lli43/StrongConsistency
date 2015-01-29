import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class Cache {

	/**
	 * Cache has two parameters, one is cacheMap which has key value pairs. Key is a string, and value is
	 * a JSON object, format is like {"q": "searchterm", "v": versionnum, "tweets": ["tw1", "tw2"]}
	 * lock is used for multi-threads safety.
	 */
	private HashMap<String, JSONObject> cacheMap;
	private MultiReaderLock lock;
	private Logger logger;
	private int totalVersion;
	private JSONArray hashtagArray;
	
	public Cache(Logger logger) {
		cacheMap = new HashMap<String, JSONObject>();
		lock = new MultiReaderLock();
		this.logger = logger;
		totalVersion = -1;
		hashtagArray = new JSONArray();
	}
//	/*
//	 * We can get JSON object via specific key by getJSONByKey
//	 */
//	public JSONObject getJSONByKey(String key) {
//		lock.lockRead();
//		logger.debug("Get the key: " + key + "'s JSON object");
//		JSONObject JSONObj = cacheMap.get(key);
//		lock.unlockRead();
//		return JSONObj;
//	}
	
	public void clearHashtagArray() {
		lock.lockWrite();
		hashtagArray.clear();
		lock.unlockWrite();
	}
	
	public void setHashtagArray(JSONArray arr) {
		lock.lockWrite();
		hashtagArray.addAll(arr);
		lock.unlockWrite();
	}
	
	public JSONArray getHashtagList() {
		JSONArray array;
		lock.lockRead();
		array = (JSONArray) hashtagArray.clone();
		lock.unlockRead();
		return array;
	}
	
	/*
	 * update the total version number
	 */
	public void setTotalVersion(int totalVersion) {
		lock.lockWrite();
		this.totalVersion = totalVersion;
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
	/*
	 * We can update JSON object by getJSONByKey
	 */
	public void updateJSON(String key, JSONObject JSONObj) {
		lock.lockWrite();
		logger.debug("Update key: " + key + "'s JSON object");
		cacheMap.put(key, JSONObj);
		lock.unlockWrite();
	}
	/*
	 * We can get the version of the cache by key, if there is no such key,
	 * return 0
	 */
	public int getVersion(String key) {
		lock.lockRead();
		logger.debug("Get the key: " + key + "'s version");
		int version = 0;
		if (cacheMap.containsKey(key)) {
			JSONObject JSONObj = cacheMap.get(key);
//			System.out.println(JSONObj.toJSONString());
//			System.out.println("===========" + JSONObj.get("v"));
			version = Integer.parseInt(JSONObj.get("v").toString());
		}
		lock.unlockRead();
		return version;
	}
	/*
	 * Check if the JSON object associated with the key in the HASHMAP exists
	 */
	public boolean containSearchTerm(String key) {
		lock.lockRead();
		boolean bool = cacheMap.containsKey(key);
		lock.unlockRead();
		return bool;
	}
	
	/*
	 * We can get JSON body String via specific key by getJSONByKey
	 */
	public String getJSONByKey(String key) {
		lock.lockRead();
		logger.debug("Get the key: " + key + "'s JSON object");
//		JSONObject JSONObj = (JSONObject)cacheMap.get(key).;
		JSONObject object = new JSONObject();
		JSONObject o = cacheMap.get(key);
		lock.unlockRead();
		object.put("q", o.get("q"));
		object.put("tweets", o.get("tweets"));
		
		return object.toJSONString();
	}
	
	/*
	 * Clear the cache
	 */
	public void clearMap() {
		lock.lockWrite();
		cacheMap.clear();
		lock.unlockWrite();
	}

}
