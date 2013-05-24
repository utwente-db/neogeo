package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


public class SqlScriptBuilder {
	
//	Statement st = c.createStatement();
//	st.addBatch(nsql.toString());
//	st.executeBatch();

	private Connection c;
	
	private final boolean debug = false;
	
	private Statement pre_stat;
	private Statement post_stat;
	
	private StringBuilder pre_str;
	private StringBuilder post_str;
	
	public SqlScriptBuilder(Connection c) throws SQLException{
		this.c = c;
		reset();
	}
	
	public void reset() throws SQLException {
		this.pre_stat = c.createStatement();
		this.post_stat = c.createStatement();
		this.pre_str = new StringBuilder();
		this.post_str = new StringBuilder();
	}
	
	public void add(String s) throws SQLException {
		if ( debug ) {
			System.out.println("|----- EXECUTE:\n"+s+"|-----\n");
			System.out.flush();
			pre_stat.addBatch(s);
			pre_stat.executeBatch();
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
		this.pre_stat.executeBatch();
		this.post_stat.executeBatch();
		//
		this.pre_stat = c.createStatement();
		this.post_stat = c.createStatement();
	}
	
	public String getScript() throws SQLException {
		return pre_str.toString() + '\n' + post_str.toString();
	}
	
}
