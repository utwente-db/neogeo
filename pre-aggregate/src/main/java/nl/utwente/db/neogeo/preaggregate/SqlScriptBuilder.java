package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;


public class SqlScriptBuilder {
        static final Logger logger = Logger.getLogger(SqlScriptBuilder.class);
	
	private Connection c;
	
	private final boolean debug = false;
	
	private Statement pre_stat;
	private Statement post_stat;
	
	private StringBuilder pre_str;
	private StringBuilder post_str;
        
        protected boolean executeDirectly = false;
	
	public SqlScriptBuilder(Connection c) throws SQLException{
		this.c = c;
		reset();
	}
        
        public void setExecuteDirectly (boolean val) {
            this.executeDirectly = val;
        }
        
        public boolean executeDirectly () {
            return this.executeDirectly;
        }
	
	public void reset() throws SQLException {
		this.pre_stat = c.createStatement();
		this.post_stat = c.createStatement();
		this.pre_str = new StringBuilder();
		this.post_str = new StringBuilder();
	}
	
	public void add(String s) throws SQLException {
		if (executeDirectly) {
                        logger.debug("Executing:\n" + s + "\n");
                        
                        long startTime = System.currentTimeMillis();
                                                
                        try {
                            pre_stat.execute(s);
                        } catch (SQLException e) {
                            System.out.println("CAUGHT: "+e);
                            e.printStackTrace();
                            System.out.println("NEXT: "+e.getNextException());
                            System.exit(0);
                        }
                        
                        long execTime = System.currentTimeMillis() - startTime;
                        logger.debug("Affected rows: " + pre_stat.getUpdateCount());
                        logger.debug("Query execution time: " + execTime + " ms");
                        
			pre_stat = c.createStatement();
		} else {
			pre_stat.addBatch(s);
		}
                
		pre_str.append(s);
	}
	
	public void addPost(String s) throws SQLException {
		// System.out.println("|-----\n"+s+"|-----\n");
		post_stat.addBatch(s);
		post_str.append(s);
	}
	
	public void newLine() {
		pre_str.append('\n');
	}
	
	public void executeBatch()  throws SQLException{
		try {
			this.pre_stat.executeBatch();
			this.post_stat.executeBatch();
		} catch (SQLException e) {
			System.out.println("CAUGHT: "+e);
			e.printStackTrace();
			System.out.println("NEXT: "+e.getNextException());
			System.exit(0);
		}
		//
		this.pre_stat = c.createStatement();
		this.post_stat = c.createStatement();
	}
	
	public String getScript() throws SQLException {
		return pre_str.toString() + '\n' + post_str.toString();
	}
	
}
