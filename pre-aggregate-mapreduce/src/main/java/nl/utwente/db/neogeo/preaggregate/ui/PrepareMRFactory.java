package nl.utwente.db.neogeo.preaggregate.ui;

import java.sql.Connection;
import java.sql.SQLException;
import nl.utwente.db.neogeo.preaggregate.PreAggregateConfig;
import nl.utwente.db.neogeo.preaggregate.SqlUtils;
import nl.utwente.db.neogeo.preaggregate.SqlUtils.DbType;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class PrepareMRFactory {
    
    public static PrepareMR getPrepareMR (Configuration conf, DbInfo dbInfo, Connection c, PreAggregateConfig config) {
        DbType dbType;
        
        try {
            dbType = SqlUtils.dbType(c);
        } catch (SQLException ex) {
            throw new UnsupportedOperationException("Unable to determine database type from connection", ex);
        }
        
        if (dbType == DbType.MONETDB) {
            return new PrepareMRMonetDB (conf, dbInfo, c, config);
        } else if (dbType == DbType.POSTGRES) {
            return new PrepareMRPostgres (conf, dbInfo, c, config);
        } else {
            throw new UnsupportedOperationException("Database type " + dbType + " not yet supported by PrepareMR");
        }
    }
    
}
