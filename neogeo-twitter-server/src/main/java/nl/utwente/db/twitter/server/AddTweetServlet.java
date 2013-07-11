package nl.utwente.db.twitter.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@MultipartConfig
public class AddTweetServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        // ClassLoader classLoader = getClass().getClassLoader();
        // File file = new File(classLoader.getResource("quest.zip").getFile());

        response.setContentType("text/html");
        String rsp = "<html><head><title>OK</title></head></html>";
        // response.setContentLength(rsp.length());
        // response.setHeader("Content-Disposition", "attachment;filename=\"" + file.getName() + "\"");

        PrintWriter writer = response.getWriter();
        writer.write(rsp);
        
        StringBuffer jsonbuff = new StringBuffer();
        String line = null;
        try {
          BufferedReader reader = request.getReader();
          while ((line = reader.readLine()) != null)
            jsonbuff.append(line);
        } catch (Exception e) { e.printStackTrace(); /*report an error*/ }
        
        // System.out.println("READ JSON: "+jsonbuff);
        // String s = Tweet.exampleTweet;
        // System.out.println("OK: "+s);

        Tweet tweet = null;
        try {
        	tweet = new Tweet(jsonbuff.toString());
        } catch (Exception e) {
        	e.printStackTrace();
        }
        System.out.println("TWEET: id="+tweet.id_str()+", tweet="+tweet.tweet());
        
//        try {
//          JSONObject jsonObject = JSONObject.fromObject(jb.toString());
//        } catch (ParseException e) {
//          crash and burn
//          throw new IOException("Error parsing JSON request string");
//        }
        
//        try {
//            byte[] arBytes = rsp.getBytes();
//            
//            // FileInputStream is = new FileInputStream(file);
//            // is.read(arBytes);
//            
//            System.out.println("#!Handled AddTweetServlet 99:doGet()");
//            
//            ServletOutputStream op = response.getOutputStream();
//            op.write(arBytes);
//            op.flush();
//        } catch (IOException e) {
//            throw new RuntimeException("Unable to download file", e);
//        }
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        doGet(request, response);
    }
}

