package nl.utwente.db.neogeo.twitter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.*;

public class WebServer implements HttpHandler {

	private static final int DEFAULT_PORT = 30001;
	
	public void handle(HttpExchange t) throws IOException {
		InputStream is = t.getRequestBody();
		
		System.out.println("HttpExchange="+t);
		String request = convertStreamToString(is);
		System.out.println("REQUEST: "+request);
		
		String response = "This is the response";
		t.sendResponseHeaders(200, response.length());
		OutputStream os = t.getResponseBody();
		os.write(response.getBytes());
		os.close();
		System.out.println("RESPONSE: "+response);
	}

	public static String convertStreamToString(java.io.InputStream is) {
	    java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
	    return s.hasNext() ? s.next() : "";
	}
	
	public static void main(String[] argv) {
		HttpServer server;
		try {
			// incomplete, make this a seprate Thread
			server = HttpServer.create(new InetSocketAddress(DEFAULT_PORT), 0);
			server.createContext("/applications/myapp", new WebServer());
			server.setExecutor(null); // creates a default executor
			server.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Server thread is created, now try to do a HTTP get from this port

	}

}
