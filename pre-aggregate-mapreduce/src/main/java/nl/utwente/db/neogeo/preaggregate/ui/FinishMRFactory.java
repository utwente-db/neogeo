/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.neogeo.preaggregate.ui;

import java.sql.Connection;
import java.sql.SQLException;
import nl.utwente.db.neogeo.preaggregate.SqlUtils;
import nl.utwente.db.neogeo.preaggregate.SqlUtils.DbType;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class FinishMRFactory {
    
    public static FinishMR getFinishMR (Configuration conf, DbInfo dbInfo, Connection c) {
        DbType dbType;
        
        try {
            dbType = SqlUtils.dbType(c);
        } catch (SQLException ex) {
            throw new UnsupportedOperationException("Unable to determine database type from connection", ex);
        }
        
        if (dbType == DbType.MONETDB) {
            return new FinishMRMonetDB(conf, dbInfo, c);
        } else {
            throw new UnsupportedOperationException("Database type " + dbType + " not yet supported by FinishMR");
        }
    }
    
}
