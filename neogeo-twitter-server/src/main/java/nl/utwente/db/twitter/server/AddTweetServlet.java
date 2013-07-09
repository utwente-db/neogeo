package nl.utwente.db.twitter.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@MultipartConfig
public class AddTweetServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("quest.zip").getFile());

        response.setContentType("application/pdf");
        response.setContentLength((int)file.length());
        response.setHeader("Content-Disposition", "attachment;filename=\"" + file.getName() + "\"");

        try {
            byte[] arBytes = new byte[(int) file.length()];
            
            FileInputStream is = new FileInputStream(file);
            is.read(arBytes);
            
            System.out.println("#!Handled AddTweetServlet:doGet()");
            
            ServletOutputStream op = response.getOutputStream();
            op.write(arBytes);
            op.flush();
        } catch (IOException e) {
            throw new RuntimeException("Unable to download file", e);
        }
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        doGet(request, response);
    }
}

