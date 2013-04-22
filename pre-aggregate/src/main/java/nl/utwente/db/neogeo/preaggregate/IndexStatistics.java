package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class IndexStatistics {

	public static final String schema = "public";
	
	Connection c;
	
	IndexStatistics(Connection c) throws SQLException {
		this.c = c;
		
	}
	
	private void create_dimTable(int dim, int N, int levels) throws SQLException {
		String tableName = "dim"+dim;
		
		if ( SqlUtils.existsTable(c, schema, tableName) )
			SqlUtils.dropTable(c, schema, tableName);
		SqlUtils.executeNORES(c, 
				"CREATE TABLE " + schema + "." + tableName + " (" +
					"level int," + 
					"factor int" +
					");");
		int factor = 0;
		for (int i=0; i<levels; i++) {
			factor = (i==0) ? 1 : (factor*N);
			// System.out.println("Level("+i+") factor ="+factor);
			SqlUtils.executeNORES(c,"INSERT INTO " + schema + "." + tableName + "  (level,factor) VALUES("+i+","+factor+");");
		}
	}
	
	public void generate(int D, int N, int levels) throws SQLException {
		int i;
		
		for(i=0; i<D; i++)
			create_dimTable(i,N,levels);
		
		int low 	= 0;
		int high	 = (int)Math.pow(N, levels-1);
		
		if ( SqlUtils.existsTable(c, schema, "data") )
			SqlUtils.dropTable(c, schema, "data");
		StringBuilder sql = new StringBuilder();
		StringBuilder attr = new StringBuilder();
		StringBuilder qm = new StringBuilder();

		sql.append("CREATE TABLE " + schema + "." + "data" + " (");
		for(i=0; i<D; i++) {
			if ( (i>0) ) {
				sql.append(',');
				attr.append(',');
				qm.append(',');
			}
			sql.append("i"+i+" int");
			attr.append("i"+i);
			qm.append("?");
		}
		sql.append(",val"+" int");
		sql.append(");");
	
		// System.out.println("--> "+sql);
		SqlUtils.executeNORES(c,sql.toString());
		
		StringBuilder from = new StringBuilder();
		StringBuilder select = new StringBuilder();
		for(i=0; i<D; i++) {
			if ( i> 0 ) {
				select.append(',');
				from.append(',');
			}
			from.append("generate_series("+low+","+(high-1)+") AS i"+i);
			select.append("i"+i);
		}
		select.append(",1 as val");
		SqlUtils.executeNORES(c,"INSERT INTO "+schema+"."+"data" + " SELECT "+select+" FROM "+from+";");
		
		select = new StringBuilder();
		from = new StringBuilder();
		StringBuilder where = new StringBuilder();
		StringBuilder gb = new StringBuilder();

		for(i=0; i<D; i++) {
			if (i>0) {
				select.append(',');
				from.append(',');
				gb.append(',');
			}
			if ( i > 1 )
				where.append(" AND ");
			select.append("dim"+i+".level AS l"+i);
			select.append(", data.i"+i+"/dim"+i+".factor AS v"+i);
			from.append("dim"+i);
			if ( i > 0 )
				where.append("dim0.level=dim"+i+".level");
			gb.append("l"+i+",v"+i);
		}
		select.append(", SUM(data.val)");
		from.append(",data");
		
		
		
		String subindexQ = "SELECT "+select + " FROM " + from + " GROUP BY "+gb;
		String regularQ;
		
		if ( where.length() > 0 ) // case of 1 dimension
			regularQ = "SELECT "+select + " FROM " + from + " WHERE " +where+" GROUP BY "+gb;
		else
			regularQ = subindexQ;
		
		long level0_cells = SqlUtils.count(c,schema,"data", "*");
		long level0_n_cells = SqlUtils.queryCount(c, regularQ);
		long all_cells = SqlUtils.queryCount(c, subindexQ);

		if ( true ) {
			System.out.println("#!dimensions="+D+", N="+N+", levels="+levels);
			System.out.println("Range = ["+ low + ".." + high + "]");
			System.out.println("level0_cells="+level0_cells);
			System.out.println("level0-n_cells="+level0_n_cells);
			System.out.println("all_cells(+subi)="+all_cells);
			System.out.println("Subindexing overhead="+(100*(all_cells-level0_n_cells))/level0_n_cells+"%");
		}
		ss.add(
				""+D,
				""+N,
				""+levels,
				"["+ low + " .. " + high + "]",
				""+level0_cells,
				""+(level0_n_cells - level0_cells),
				""+(all_cells-level0_n_cells),
				""+(long)(100*(((double)all_cells-(double)level0_n_cells)/(double)level0_n_cells))+"%"
		);
		// System.out.println("Regular, level0_cells="+level0_cells+", cell_count="+regular_cells+"\n"+regularQ);
		// System.out.println("Subindexed, count="+subindex_cells+"\n"+subindexQ);
	}
	
	static final String measurements[][] = {
		{"#dim","align=\"right\""},
		{"N","align=\"right\""},
		{"levels","align=\"right\""},
		{"range","align=\"right\""},
		{"#level0","align=\"right\""},
		{"#level1-n","align=\"right\""},
		{"#subindex","align=\"right\""},
		{"overhead","align=\"right\""}
	};
	
	static SpreadSheet ss = null;
	
//	public static void main(String[] argv) {
//		try {
//			Connection c = HibernateUtils.getJDBCConnection();
//			IndexStatistics is = new IndexStatistics(c);
//			ss = new SpreadSheet();
//			ss.start(measurements);
//			for(int dd=3; dd<=3; dd++) {
//				for(int nn=3; nn<=3; nn+=1) {
//					for(int ll=3; ll<=5; ll++) {
//						is.generate( dd/* dim */, nn/* N */, ll/* levels */);
//					}
//				}
//			}
//			ss.finish("/Users/flokstra","results.html");
//			c.close();
//			System.out.println("#!FINISHED");
//		} catch (SQLException e) {
//			System.out.println("Caught: " + e);
//			e.printStackTrace(System.out);
//		}
//	}
	
}
