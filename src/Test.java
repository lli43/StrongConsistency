import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;


public class Test {

	private static int i = 0;
	public static void main(String[] args) {
		try {
			Socket sock = new Socket("localhost",5030);
			String line = null;
			String requestLine = null;
			BufferedReader in;
			in = new BufferedReader(
					new InputStreamReader(sock.getInputStream()));
			while (!(line = in.readLine().trim()).equals("")) {
				// System.out.println(line);
				requestLine = requestLine + line + "\n";
			}
			sock.close();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			System.out.println(i + "helloworld");
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(i + "helloworld");
		}
	}

	
}
