package nl.utwente.db.neogeo.twitter.harvest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Vector;

/**
 * 
 * @author ZhuZ
 * 
 *         This class includes the methods for HTTP requests.
 */

public class HTTPRequest {

	/**
	 * This method posts data to a server by POST request and returns the stream
	 * replied by the server. This method can be used when the server's reply is
	 * a long-alive stream. If setting the paramters "url" and "password" then
	 * HTTP Basic Authentication will be used for authenticating.
	 * 
	 * @author ZhuZ
	 * @param - params : a set of parameters. Example: "count=100"
	 * 
	 */
	/*
	 * public static InputStream streamByPOST( String data, String user, String
	 * password) { try{ URL url = new URL(strURL);
	 * 
	 * // open the connection HttpURLConnection urlConnection =
	 * (HttpURLConnection) url.openConnection();
	 * 
	 * // set properties of the connection
	 * urlConnection.setRequestMethod("POST"); urlConnection.setDoInput(true);
	 * urlConnection.setDoOutput(true); urlConnection.setUseCaches(false);
	 * urlConnection.setAllowUserInteraction(false);
	 * urlConnection.setRequestProperty("Content-type", "text/xml; charset=" +
	 * "UTF-8");
	 * 
	 * // set user name and password
	 * urlConnection.setRequestProperty("Authorization", "Basic " + encode(user
	 * + ":" + password));
	 * 
	 * // output stream : post data to the connection OutputStream out =
	 * urlConnection.getOutputStream(); try { Writer writer = new
	 * OutputStreamWriter(out, "UTF-8"); pipe(data, writer); writer.close(); }
	 * catch (IOException e) { throw new
	 * Exception("IOException while posting data", e); } finally { if (out !=
	 * null) out.close(); }
	 * 
	 * //input stream : read the data returned by the server InputStream in =
	 * urlConnection.getInputStream(); return in;
	 * 
	 * } catch (Exception e) { e.printStackTrace(); }
	 * 
	 * }
	 */

	public static String encode(String source) {
		sun.misc.BASE64Encoder enc = new sun.misc.BASE64Encoder();
		return (enc.encode(source.getBytes()));
	}

	/**
	 * This method sends request to a server by GET request and returns the
	 * stream replied by the server. This method can be used when the server's
	 * reply is a long-alive stream. If setting the paramters "url" and
	 * "password" then HTTP Basic Authentication will be used for
	 * authenticating.
	 * 
	 * @author ZhuZ
	 * @param - params : a set of parameters. Example: "count=100"
	 * 
	 */
	public static InputStream streamByGET(String url, String user,
			String password, String... params) {
		Vector<String> vecParams = new Vector<String>();
		if (params.length > 0) {
			for (String param : params)
				vecParams.add(param);
		}
		String formattedURL = formatRequest(url, vecParams);
		if (formattedURL.startsWith("https://")
				|| formattedURL.startsWith("http://")) {
			try {
				URL u = new URL(formattedURL);
				HttpURLConnection conn = (HttpURLConnection) u.openConnection();
				if (user != null && password != null) {
					String userPassword = user + ":" + password;
					String encoding = new sun.misc.BASE64Encoder()
							.encode(userPassword.getBytes());
					conn.setRequestProperty("Authorization", "Basic "
							+ encoding);
				}
				return conn.getInputStream();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	/**
	 * Combine the endpoint and the parameters to form a GET request. For
	 * example:
	 * 
	 * endpoint: http://api.twitter.com/reverse_geocode.json parameter1:
	 * lat=37.781157 parameter2: accuracy=0
	 * 
	 * the combined request:
	 * http://api.twitter.com/reverse_geocode.json?lat=37.781157&accuracy=0
	 */

	public static String formatRequest(String endpoint, Vector<String> params) {
		System.out.println("Endpoint: " + endpoint);
		for (String param : params)
			System.out.println("Parameter: " + param);

		StringBuilder result = null;

		if (endpoint != null && endpoint.length() > 0) {
			result = new StringBuilder();
			result.append(endpoint);
		} else {
			return null;
		}

		if (params != null) {
			result.append("?");
			for (int i = 0; i < params.size(); ++i) {
				result.append(params.get(i));
				if (i != params.size() - 1)
					result.append("&");
			}
		}

		return result.toString();
	}

	/**
	 * Send a GET request and return the results
	 * 
	 * @param getRequest
	 *            - The GET request Example:
	 *            "http://api.twitter.com/1/geo/id/df51dec6f4ee2b2c.json"
	 * @return the results returned by the server. null if no return.
	 * 
	 * @throws Exception
	 */
	public static String sendGetRequest(String getRequest) {
		System.out.println(getRequest);
		String result = null;
		if (getRequest.startsWith("http://")) {
			try {
				URL url = new URL(getRequest);
				URLConnection conn = url.openConnection();
				BufferedReader rd = new BufferedReader(new InputStreamReader(
						conn.getInputStream()));
				StringBuffer sb = new StringBuffer();
				String line;
				while ((line = rd.readLine()) != null) {
					sb.append(line);
				}
				rd.close();
				result = sb.toString();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	/**
	 * Reads data from the data reader and posts it to a server via POST
	 * request.
	 * 
	 * @param data
	 *            - The data you want to send
	 * @param endpoint
	 *            - The server's address
	 * @param output
	 *            - writes the server's response to output
	 * 
	 * @throws Exception
	 */
	public static void postData(Reader data, URL endpoint, Writer output)
			throws Exception {
		HttpURLConnection urlc = null;
		try {
			urlc = (HttpURLConnection) endpoint.openConnection();
			try {
				urlc.setRequestMethod("POST");
			} catch (ProtocolException e) {
				throw new Exception(
						"Shouldn't happen: HttpURLConnection doesn't support POST??",
						e);
			}
			urlc.setDoOutput(true);
			urlc.setDoInput(true);
			urlc.setUseCaches(false);
			urlc.setAllowUserInteraction(false);
			urlc.setRequestProperty("Content-type", "text/xml; charset="
					+ "UTF-8");
			OutputStream out = urlc.getOutputStream();
			try {
				Writer writer = new OutputStreamWriter(out, "UTF-8");
				pipe(data, writer);
				writer.close();
			} catch (IOException e) {
				throw new Exception("IOException while posting data", e);
			} finally {
				if (out != null)
					out.close();
			}
			InputStream in = urlc.getInputStream();
			try {
				Reader reader = new InputStreamReader(in);
				pipe(reader, output);
				reader.close();
			} catch (IOException e) {
				throw new Exception("IOException while reading response", e);
			} finally {
				if (in != null)
					in.close();
			}
		} catch (IOException e) {
			throw new Exception("Connection error (is server running at "
					+ endpoint + " ?): " + e);
		} finally {
			if (urlc != null)
				urlc.disconnect();
		}
	}

	/**
	 * Pipes everything from the reader to the writer via a buffer
	 */
	private static void pipe(Reader reader, Writer writer) throws IOException {
		char[] buf = new char[1024];
		int read = 0;
		while ((read = reader.read(buf)) >= 0) {
			writer.write(buf, 0, read);
		}
		writer.flush();
	}

}
