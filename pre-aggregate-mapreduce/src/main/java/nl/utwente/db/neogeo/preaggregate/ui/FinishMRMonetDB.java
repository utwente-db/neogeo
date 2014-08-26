package nl.utwente.db.neogeo.preaggregate.ui;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.cwi.monetdb.mcl.net.MapiSocket;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.utwente.db.neogeo.preaggregate.SqlUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class FinishMRMonetDB extends FinishMR {
    static final Logger logger = Logger.getLogger(FinishMRMonetDB.class);
    
    protected BufferedMCLReader mapiIn;
    protected BufferedMCLWriter mapiOut;
    protected MapiSocket server;
    
    public FinishMRMonetDB (Configuration conf, DbInfo dbInfo, Connection c) {
        super(conf, dbInfo, c);
    }
    
    @Override
    protected void prepareInsertData (String indexTable) throws IOException, FinishException {
        server = new MapiSocket();

        server.setDatabase(dbInfo.getDatabase());
        server.setLanguage("sql");
        
        List warning = null;
        try {
            warning = server.connect(dbInfo.getHostname(), dbInfo.getPort(), dbInfo.getUsername(), dbInfo.getPassword());
        } catch (MCLParseException ex) {
            throw new FinishException("Unable to connect to MonetDB server via MAPI socket", ex);
        } catch (MCLException ex) {
            throw new FinishException("Unable to connect to MonetDB server via MAPI socket", ex);
        }
        
        if (warning != null) {
            for (Iterator it = warning.iterator(); it.hasNext();) {
                logger.warn(it.next().toString());
            }
        }
        
        mapiIn = server.getReader();
        mapiOut = server.getWriter();
        
        String error = mapiIn.waitForPrompt();
        if (error != null) {
            throw new FinishException(error);
        }
        
        // the leading 's' is essential, since it is a protocol
        // marker that should not be omitted, likewise the
        // trailing semicolon
        mapiOut.write('s');

        String copyQuery = "COPY INTO " + indexTable + " FROM STDIN USING DELIMITERS ',','\\n';";

        mapiOut.write(copyQuery);
        mapiOut.newLine();
    }
        
    @Override
    protected void finishInsertData () throws IOException, FinishException {
        mapiOut.writeLine(""); // need this one for synchronisation over flush()
        String error = mapiIn.waitForPrompt();
        if (error != null) throw new FinishException(error);
        
        // disconnect from server
        server.close();
    }
    
    @Override
    protected void insertDataIntoTable (InputStream in) throws IOException {    
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        
        String line;
        while((line = reader.readLine()) != null) {
            mapiOut.write(line);
            mapiOut.newLine();
        }
    }
}
