package nl.utwente.db.neogeo.preaggregate.ui;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.cwi.monetdb.mcl.net.MapiSocket;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.utwente.db.neogeo.preaggregate.PreAggregateConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class PrepareMRMonetDB extends PrepareMR {
    static final Logger logger = Logger.getLogger(PrepareMRMonetDB.class);
    
    protected MapiSocket server;
    
    protected BufferedMCLReader mapiIn;
    
    protected BufferedMCLWriter mapiOut;

    public PrepareMRMonetDB (Configuration conf, DbInfo dbInfo, Connection c, PreAggregateConfig config) {
        super(conf, dbInfo, c, config);
    }
    
    @Override
    protected void prepareCreateChunks () throws PrepareException, IOException {
        server = new MapiSocket();

        server.setDatabase(dbInfo.getDatabase());
        server.setLanguage("sql");
        
        List warning = null;
        try {
            warning = server.connect(dbInfo.getHostname(), dbInfo.getPort(), dbInfo.getUsername(), dbInfo.getPassword());
        } catch (MCLParseException ex) {
            throw new PrepareException("Unable to connect to MonetDB server via MAPI socket", ex);
        } catch (MCLException ex) {
            throw new PrepareException("Unable to connect to MonetDB server via MAPI socket", ex);
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
            throw new PrepareException(error);
        }
    }
    
    @Override
    protected int writeChunk(StringBuilder where, String fileName, int chunkNum) throws IOException, SQLException {
        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempPath.getAbsolutePath() + fileName), "utf-8"));
        
        String chunkSelectQuery = this.select_level0(c, table, where.toString(), axis, aggregateColumn, aggregateMask);
        
        int ret = 0;
        // the leading 's' is essential, since it is a protocol
        // marker that should not be omitted, likewise the
        // trailing semicolon
        mapiOut.write('s');

        String copyQuery = "COPY " + chunkSelectQuery + " INTO STDOUT USING DELIMITERS ',','\\n';";

        mapiOut.write(copyQuery);
        mapiOut.newLine();
        mapiOut.writeLine("");

        String line;
        while((line = mapiIn.readLine()) != null) {
            int lineType = mapiIn.getLineType();

            // when PROMPT is reached all data has been read
            if (lineType == BufferedMCLReader.PROMPT) break;

            // ignore all other official lines
            if (lineType != 0) continue;

           writer.write(line);
           writer.write("\n");
           ret++;
        }
        
        // finalize chunk
        writer.close();       
        
        return ret;
    }
    
    @Override
    protected void finishCreateChunks () throws PrepareException, IOException {
        server.close();
    }
    
}
