package org.geotools.data.aggregation;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

import nl.utwente.db.neogeo.preaggregate.PreAggregate;
import nl.utwente.db.neogeo.preaggregate.SqlUtils.DbType;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.util.KVP;

// Referenced classes of package org.geotools.data.aggregation:
//            AggregationDataStore

public class AggregationDataStoreFactory implements DataStoreFactorySpi {  
    private static final Logger LOGGER = Logger.getLogger("org.geotools.data.aggregation.AggregationDataStoreFactory");
    //private static final String FILE_TYPE = "csv";
//    public static final Param FILE_PARAM = new org.geotools.data.DataAccessFactory.Param("file", File.class, "csv file", true, null, new KVP(new Object[] {
//        "ext", "csv"
//    }));
    
   
    public static final Param DBTYPE_PARAM = new Param("Database type", String.class, "Type of database backend", true, DbType.POSTGRES.toString(), new KVP(new Object[] {
        "options", new ArrayList<String>() {{add(DbType.POSTGRES.toString()); add(DbType.MONETDB.toString());}}
    }));
    public static final Param HOSTNAME_PARAM = new Param("hostname", String.class, "hostname", true, "localhost");
    public static final Param PORT_PARAM = new Param("port", Integer.class, "port number", true, 5432);
    public static final Param SCHEMA_PARAM = new Param("schema", String.class, "PostGIS schema", true, "public");
    public static final Param DATABASE_PARAM = new Param("database", String.class, "PostGIS database name", true);
    public static final Param USERNAME_PARAM = new Param("username", String.class, "username", true, "geoserver");
    public static final Param PASSWORD_PARAM = new Param("password", String.class, "password", true, null, new KVP(new Object[] {
        "isPassword", Boolean.valueOf(true)
    }));
    public static final Param X_SIZE_PARAM = new Param("xSize", Integer.class, "number of grid cells in x direction", true, 10);
    public static final Param Y_SIZE_PARAM = new Param("ySize", Integer.class, "number of grid cells in y direction", true, 10);
    public static final Param TIME_SIZE_PARAM = new Param("timeSize", Integer.class, "number of bins for the time dimension", true, 1);
    public static final Param CNT_AGG_PARAM = new Param("count", Boolean.class, "output count aggregate", true, true);
    public static final Param SUM_AGG_PARAM = new Param("sum", Boolean.class, "output sum aggregate", true, false);
    public static final Param MIN_AGG_PARAM = new Param("minimum", Boolean.class, "output min aggregate", true, false);
    public static final Param MAX_AGG_PARAM = new Param("maximum", Boolean.class, "output maxt aggregate", true, false);
    
    public AggregationDataStoreFactory() {
    }

    public String getDisplayName() {
        return "aggregate";
    }

    public String getDescription()    {
        return "PostGIS aggregation index query";
    }

    public Param[] getParametersInfo()
    {
        return (new Param[] {
            DBTYPE_PARAM, HOSTNAME_PARAM, PORT_PARAM, SCHEMA_PARAM, DATABASE_PARAM, USERNAME_PARAM, PASSWORD_PARAM,
            X_SIZE_PARAM,Y_SIZE_PARAM,TIME_SIZE_PARAM,CNT_AGG_PARAM,SUM_AGG_PARAM,MIN_AGG_PARAM,MAX_AGG_PARAM
        });
    }

    public boolean canProcess(Map params) {
        DbType dbType;
        String hostname;
        int port;
        String schema;
        String database;
        String username;
        String password;
        java.sql.Connection connection;
        
        try {
            dbType = lookUpDbType(params);            
            hostname = (String)HOSTNAME_PARAM.lookUp(params);
            port = ((Integer)PORT_PARAM.lookUp(params)).intValue();
            schema = (String)SCHEMA_PARAM.lookUp(params);
            database = (String)DATABASE_PARAM.lookUp(params);
            username = (String)USERNAME_PARAM.lookUp(params);
            password = (String)PASSWORD_PARAM.lookUp(params);
            
            String className = "";
            if (dbType == DbType.POSTGRES) {
                className = "org.postgresql.Driver";
            } else if (dbType == DbType.MONETDB) {
                className = "nl.cwi.monetdb.jdbc.MonetDriver";
            }
            
            try {
                Class.forName(className);
            } catch(ClassNotFoundException e) {
                LOGGER.severe("Where is your JDBC Driver (" + className + ")? Include in your library path!");
                e.printStackTrace();
                return false;
            }
            
        } catch(IOException e) {
            return false;
        }
        LOGGER.severe("JDBC Driver Registered!");
        connection = null;
        try {
            StringBuilder connUrl = new StringBuilder();
            if (dbType == DbType.POSTGRES) {
                connUrl.append("jdbc:postgresql://");
            } else if (dbType == DbType.MONETDB) {
                connUrl.append("jdbc:monetdb://");
            }
            
            connUrl.append(hostname).append(":").append(port).append("/").append(database);            
            
            connection = DriverManager.getConnection(connUrl.toString(), username, password);
        } catch(SQLException e){
            LOGGER.severe("Connection Failed! Check output console");
            e.printStackTrace();
            return false;
        }
        return connection != null;
    }

    public boolean isAvailable() {
        return true;
    }

    public Map getImplementationHints() {
        return null;
    }

    public DataStore createDataStore(Map params) throws IOException {
        DbType dbType = lookUpDbType(params);
        String hostname = (String)HOSTNAME_PARAM.lookUp(params);
        int port = ((Integer)PORT_PARAM.lookUp(params)).intValue();
        String schema = (String)SCHEMA_PARAM.lookUp(params);
        String database = (String)DATABASE_PARAM.lookUp(params);
        String username = (String)USERNAME_PARAM.lookUp(params);
        String password = (String)PASSWORD_PARAM.lookUp(params);
        int xSize = ((Integer)X_SIZE_PARAM.lookUp(params)).intValue();
        int ySize = ((Integer)Y_SIZE_PARAM.lookUp(params)).intValue();
        int timeSize = ((Integer)TIME_SIZE_PARAM.lookUp(params)).intValue();
        boolean cnt = ((Boolean)CNT_AGG_PARAM.lookUp(params)).booleanValue();
        boolean sum = ((Boolean)SUM_AGG_PARAM.lookUp(params)).booleanValue();
        boolean min = ((Boolean)MAX_AGG_PARAM.lookUp(params)).booleanValue();
        boolean max = ((Boolean)MIN_AGG_PARAM.lookUp(params)).booleanValue();
        int mask =  cnt ? PreAggregate.AGGR_COUNT:0;
        mask += sum ? PreAggregate.AGGR_SUM:0;
        mask += min ? PreAggregate.AGGR_MIN:0;
        mask += max ? PreAggregate.AGGR_MAX:0;
        return new AggregationDataStore(dbType, hostname, port, schema, database, username, password, xSize, ySize, timeSize, mask);
    }

    public DataStore createNewDataStore(Map params) throws IOException {
        throw new UnsupportedOperationException("AggregationDataStore is read-only");
    }
    
    protected DbType lookUpDbType (Map params) throws IOException {
        DbType ret = null;
        String typeStr = ((String)DBTYPE_PARAM.lookUp(params)).toUpperCase();
            
        if (typeStr.equals(DbType.POSTGRES.toString())) {
            ret = DbType.POSTGRES;
        } else if (typeStr.equals(DbType.MONETDB.toString())) {
            ret = DbType.MONETDB;
        } else {
            throw new UnsupportedOperationException("Database type '" + typeStr + "' not supported!");
        }        
        
        return ret;
    }

}
