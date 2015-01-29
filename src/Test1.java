import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Test1 {
	
	private int times = 0;
	public static void main(String[] args) {
		final Timer timer = new Timer(true);
		
//		String content = "{\"localhost:5001\":0\"localhost:5002\":1,\"localhost:5003\":2}";
//		JSONObject jobj = new JSONObject();
//		try {
//			JSONParser jParser = new JSONParser();
//			Object obj = jParser.parse(content);
//			jobj = (JSONObject)obj;
////			System.out.println(jobj.toJSONString());
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//		System.out.println(jobj.toJSONString());
////		for (int i = 0; i < jobj.size(); i++) {
////			jobj.keySet().
////		}
//		for (Object key : jobj.keySet()) {
//			if (Integer.parseInt(jobj.get(key).toString()) > 1) {
//				System.out.println((String)key);
//			}
//		}
//		String a = "aaaaaaaa";
//		String b = "bbbbbbbb";
//		a = b;
//		b = "ccccccc";
//		System.out.println(a + b);
		ArrayList l = new ArrayList();
		for (int i = 0; i<10;i++) {
			l.add(i);
		}
		int different = 2;
		
		System.out.println(l.subList(l.size() - different - 1, l.size() - 1).size());
		System.out.println(l.size());
//		TimerTask task = new TimerTask() {     
//		    public void run() {    
//		    	int times = 0;
//		    	System.out.println("waiting 1 sec");
//		    	times ++;
//		    	if (times > 3) {
//		    		timer.cancel();
//		    	}
//		    }     
//		};
//		timer.schedule(task, 0, 1000);
//		
//		while (true) {}
	}
		
}