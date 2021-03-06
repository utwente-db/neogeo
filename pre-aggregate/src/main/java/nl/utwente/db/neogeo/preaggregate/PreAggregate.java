package nl.utwente.db.neogeo.preaggregate;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Vector;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.AxisIndexer;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.DoubleAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.IntegerAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.LongAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.TimestampAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.SqlUtils.DbType;
import org.apache.log4j.Logger;

public class PreAggregate {
	private static final Logger LOGGER = Logger.getLogger("org.geotools.data.aggregation.PreAggregate");
	
	private final boolean gen_optimized = true;
        
	/*
	 * Experiment setup variables
	 * 
	 */
        public static final boolean     showAxisAndKey          = true;
	public static final boolean	doResultCorrection	= true;
        public static final boolean     DEFAULT_SERVERSIDE_STAIRWALK = false;
        
        public static final boolean executeQueriesDirectly      = true;
	public static final char	DEFAULT_KD			= AggrKeyDescriptor.KD_CROSSPRODUCT_LONG;

	private	static final int	AGGR_BASE			= 0x01;
	public static final	int		AGGR_COUNT			= AGGR_BASE;
	public static final	int		AGGR_SUM			= AGGR_BASE << 1;
	public static final	int		AGGR_MIN			= AGGR_BASE << 2;
	public static final	int		AGGR_MAX			= AGGR_BASE << 3;
	public static final int		AGGR_ALL			= (AGGR_COUNT|AGGR_SUM|AGGR_MIN|AGGR_MAX);

	protected int	aggregateMask = 0; // the 'allowed' aggregates for this instance

	protected static final String  aggregateRepositoryName	= "pre_aggregate";
	protected static final String  aggregateRepositoryDimName	= "pre_aggregate_axis";

	public static final int	RMIN	= 0;
	public static final int	RMAX	= 1;

	private static final boolean useDirect = true; // do direct texting in postgis without aggregate

	public static final boolean do_assert		= false;
	public static final long    indexMask		= 0x00FFFFFFFFFFFFFFL; // warning, should match with levStart
	public static final int		levStart 		= 56; // the first 8 bits

	public static final long li_key(short l, int i) {
		long key = (((long)l)<<levStart) | (long)i;

		if ( do_assert ) {
			if ( l != li_l(key) )
				throw new RuntimeException("li_l(): " + l + "<>" + li_l(key));
			if ( i != li_i(key) )
				throw new RuntimeException("li_i(l="+l+", i="+i+"): i=" + i + "<> li()=" + li_i(key) + ", bits="+MetricAxis.log2(i));
		}
		return key;
	}

	public static final short li_l(long key) {
		return (short)(key >>levStart);
	}

	public static final int li_i(long key) {
		return (int)(key & indexMask);
	}

	public static final String li_toString(long key) {
		return "li_key[l="+li_l(key)+", i="+li_i(key) + "]";
	}

	public static final String PA_EXTENSION = "_pa";

	protected Connection c;
	protected String schema;
	protected String table;
	protected String label;
        
        protected boolean serversideStairwalk = DEFAULT_SERVERSIDE_STAIRWALK;

	AggrKeyDescriptor kd = null;
	String indexPrefix = "_ipfx_"; // incomplete, should be unique in database

	private HashMap<Long, AggrRec> internalHash = null;

	protected AggregateAxis axis[];
	protected String aggregateColumn;
	protected String aggregateType;

	public PreAggregate() {
	}

	public PreAggregate(Connection c, String schema, String table, String label) 
	throws SQLException {
		_init(c,schema,table,label);
	}

	// Is this still used?
	public PreAggregate(Connection c, String schema, String table, String label, HashMap<Long, AggrRec> internalHash)
	throws SQLException {
		_init(c,schema,table,label);
		this.internalHash = internalHash;
	}

	public PreAggregate(Connection c, String schema, String table, String override_name, String label, AggregateAxis axis[], String aggregateColumn, String aggregateType, int aggregateMask, int axisToSplit, long chunkSize, Object[][] newRange) 
	throws SQLException {
        	createPreAggregate(c,schema,table,override_name, label,axis,aggregateColumn,aggregateType,aggregateMask,axisToSplit,chunkSize,newRange);
	}

	private void _init(Connection c, String schema, String table, String label)
	throws SQLException {
		this.c = c;
		this.schema = schema;
		this.table = table;
		this.label = label;
		if (!read_repository(c,schema,table,label) )
			throw new SQLException("No PreAggregate " + label + " for table " + schema + "."
					+ table);
	}
        
        public void enableServersideStairwalk(boolean val) {
            this.serversideStairwalk = val;
        }
        
        /**
         * Checks if the geometry_columns table exists and if a table/column has been properly registered
         * 
         * @param c
         * @param schema
         * @param table
         * @param point_column
         * @throws SQLException 
         */
        protected void checkMonetDbGis(Connection c, String schema, String table, String point_column) throws SQLException {
            // check if geometry_columns table exists
            if (SqlUtils.existsTable(c, schema, "geometry_columns") == false) {
                throw new SQLException("GeoSpatial table `geometry_columns` does not exist in database. Create if first by executing the create_geometry_columns_monetdb.sql file!");
            }
            
            // check if point column is registered in geometry_columns table
            PreparedStatement stmt = c.prepareStatement("SELECT * from geometry_columns WHERE LOWER(f_table_schema) = ? AND LOWER(f_table_name) = ? AND LOWER(f_geometry_column) = ?");
            stmt.setString(1, schema.toLowerCase());
            stmt.setString(2, table.toLowerCase());
            stmt.setString(3, point_column.toLowerCase());
            
            ResultSet res = stmt.executeQuery();
            
            if (res.next() == false) {
                res.close();
                stmt.close();
                
                
                // Example:
                // INSERT INTO geometry_columns VALUES ('sys', 'london_hav_neogeo', 'coordinates', 2, 4326, 'POINT');
                throw new SQLException("Point column `" + point_column + "` for table `" + schema + "." + table + "` is not "
                        + "registered in geometry_columns table! Manually insert it into the geometry_columns table first.");
            }
            
            res.close();
            stmt.close();
        }

	public Object[][] getRangeValues(Connection c) throws SQLException {
		return getRangeValues(c, schema, table, axis);
	}

	public Object[][] getRangeValues(Connection c, String schema, String table, AggregateAxis axis[]) throws SQLException {
		int i;
		Object res[][] = new Object[axis.length][2];

		StringBuilder aggrs = new StringBuilder();
		for(i=0; i<axis.length; i++ ) {
			if ( i>0 )
				aggrs.append(',');
			aggrs.append("min(");
			aggrs.append(axis[i].columnExpression());
			aggrs.append("),max(");
			aggrs.append(axis[i].columnExpression());
			aggrs.append(")");
		}
		// incomplete, do nothing with the count yet
		ResultSet rs = SqlUtils.execute(c,"SELECT "+ aggrs + ",count(*) FROM " + schema + "." + table + ";"); 
		if ( !rs.next() )
			throw new SQLException("PreAggregate: unable to get range values");
		// System.out.println("#!Executing: "+"SELECT "+ aggrs + ",count(*) FROM " + schema + "." + table + ";");
		for(i=0; i<axis.length; i++ ) {
			res[i][RMIN] = rs.getObject(1+i*2); // min
			res[i][RMAX] = rs.getObject(2+i*2); // max
		}
		return res;
	}

	protected String rangeFunName(int dim) {
		return indexPrefix + "d"+dim+"rf";
	}

	protected String create_dimTable(Connection c, String schema, int dim, int N, int levels, SqlScriptBuilder sql_build) throws SQLException {
		String tableName = indexPrefix + "dim"+dim;

                if (SqlUtils.dbType(c) == DbType.MONETDB) {
                    if (SqlUtils.existsTable(c, schema, tableName)) {
                        sql_build.add("DROP TABLE " + schema + "." + tableName + ";\n");
                    }                    
                } else {
                    sql_build.add("DROP TABLE IF EXISTS " + schema + "." + tableName + ";\n");
                }                
		
		sql_build.add("CREATE TABLE " + schema + "." + tableName + " (" + "level int," + "factor int" + ");\n");
		int factor = 0;
		for (int i=0; i<levels; i++) {
			factor = (i==0) ? 1 : (factor*N);
			sql_build.add("INSERT INTO " + schema + "." + tableName + "  (level,factor) VALUES("+i+","+factor+");\n");
		}
		sql_build.addPost("DROP TABLE " + schema + "." + tableName + ";\n");
		return tableName;
	}
        
        protected short initializeAxis (String table, AggregateAxis[] axis) throws SQLException {
            int i;
            
            if (showAxisAndKey)
                LOGGER.debug("Aggregate Axis:");
            
            Object obj_ranges[][] = getRangeValues(c,schema,table,axis);
            short maxLevel = 0;
            for (i = 0; i < axis.length; i++) {
                    if (axis[i].isMetric()) {
                            MetricAxis metric = (MetricAxis) axis[i];

                            if (metric.hasRangeValues()) {
                                    if ((metric.getIndex(obj_ranges[i][RMIN], true) < 0) || 
                                            (metric.getIndex(obj_ranges[i][RMAX], true) < 0))
                                            throw new RuntimeException("predefined ranges conflict with min/max dataset");
                            } else {
                                    metric.setRangeValues(obj_ranges[i][RMIN], obj_ranges[i][RMAX]);
                            }

                            /*
                             * Adjust the axis a little bit too wide on blocksize multiples
                             */
                            Object wide_min, wide_max;

                            wide_min = metric.reverseValue(-1);
                            wide_max = metric.reverseValue(axis[i].axisSize());
                            // System.out.println("#!OLD    AXIS: "+axis[i]);
                            metric.setRangeValues(wide_min, wide_max);
                            // System.out.println("#!ADJUST AXIS: "+axis[i]);
                    }
                    if ( axis[i].maxLevels() > maxLevel )
                            maxLevel = axis[i].maxLevels();
                    
                    if (showAxisAndKey)
                        LOGGER.debug("AXIS["+i+"]="+axis[i]);
            }
            obj_ranges = null;
            
            return maxLevel;
        }

	protected void createPreAggregate(Connection c, String schema,
			String table, String override_name, String label, AggregateAxis axis[],
			String aggregateColumn, String aggregateType, int aggregateMask, 
			int i_axisToSplit, long chunkSize, Object[][] DELnewRange) throws SQLException {
		int i;
		String dimTable[]	 = new String[axis.length];
                
                this.c = c;
                this.schema = schema;
                this.table = table;
                this.axis = axis;
                this.aggregateColumn = aggregateColumn;
                this.aggregateType = aggregateType;
                this.aggregateMask = aggregateMask;
                
                // Ensure that MonetDB has the necessary additional functions
                if (SqlUtils.dbType(c) == DbType.MONETDB) {
                    SqlUtils.compatMonetDb(c);
                }

		long create_time_ms = new Date().getTime();
                
		/*
		 * First initialize and compute the aggregation axis
		 */
		short maxLevel = initializeAxis(table, axis);
                
                try {
                    kd = new AggrKeyDescriptor(DEFAULT_KD, axis);
                } catch (AggrKeyDescriptor.TooManyBitsException ex) {
                    LOGGER.warn("Bits of key > 63, switching to ByteString key. Consider reducing number of dimensions of baseblocksize!");
                    
                    try {                        
                        kd = new AggrKeyDescriptor(AggrKeyDescriptor.KD_BYTE_STRING, axis);
                    } catch (AggrKeyDescriptor.TooManyBitsException ex1) {
                        throw new RuntimeException("Unable to switch to ByteString key, still too many bits!");
                    }
                }
                
                // check if database has necessary supporting functions to generate aggregate keys
                if (kd.checkForSupportFunctions(c) == false ) {
                    if (SqlUtils.dbType(c) == DbType.MONETDB && kd.kind() == AggrKeyDescriptor.KD_BYTE_STRING) {
                        throw new RuntimeException("MonetDB needs C-based NeoGeo extension to be able to generate ByteString AggregateKeys. Make sure it is installed first!");
                    } else {
                        throw new RuntimeException("Database is missing necessary support functions to be able to generate AggregateKeys");
                    }
                }
                
                if (showAxisAndKey)
                    LOGGER.debug("KEY="+kd);

		/*
		 * Start generating the SQL commands for the pre aggregate index creation. 
		 * The main code is generated in pre_script. The cleanup code is generated 
		 * in post_script.
		 */
		SqlScriptBuilder sql_build = new SqlScriptBuilder(c);
                
                sql_build.setExecuteDirectly(executeQueriesDirectly);
                                
		for(i=0; i<axis.length; i++) {
			// generate the range conversion function for the dimension
			sql_build.add(axis[i].sqlRangeFunction(c, rangeFunName(i)));
			sql_build.newLine();

			sql_build.addPost(SqlUtils.gen_DROP_FUNCTION(c, rangeFunName(i),axis[i].sqlType()));

			// generate the dimension level/factor value table
			dimTable[i] = create_dimTable(c, schema,i,axis[i].N(),axis[i].maxLevels(), sql_build);
			sql_build.newLine();
		}

		// generate the function which converts all dimension levels/range indices into one value
		String genKey = indexPrefix+"genKey";                
                kd.createKeyFunction (c, genKey, sql_build); 

                // create the table that will hold the final index
		String table_pa = create_index_table(sql_build, override_name, kd);

		String lfp_table = schema + "." + indexPrefix + "lfp";
		if ( gen_optimized ) {
			sql_build.add("-- generate table with all level/factor possibilities\n");
                        
                        if (SqlUtils.dbType(c) == DbType.MONETDB) {
                            if (SqlUtils.existsTable(c, schema, lfp_table)) {
                                sql_build.add("DROP TABLE " + lfp_table + ";\n");
                            }
                        } else {                        
                            sql_build.add("DROP TABLE IF EXISTS " + lfp_table + ";\n");
                        }
                        
			gen_lfp_table(c,sql_build, lfp_table,axis,dimTable);
		}
		
		int nChunks = 1;
		Object ro[][] = null;
		MetricAxis axisToSplit = null;
		if ( i_axisToSplit >= 0 ) {
			if ( axis[i_axisToSplit].isMetric() )
				axisToSplit = (MetricAxis)axis[i_axisToSplit];
			else
				throw new SQLException("unable to split over non-metric axis: "+i_axisToSplit);
                        
                        LOGGER.info("Counting number of tuples in table to determine nr. of chunks...");
                        
			long nTuples = SqlUtils.count(c,schema,table,"*");
                        
                        LOGGER.info("Found " + nTuples + " tuples in table");                        
                        
			nChunks = (int) (nTuples / chunkSize) +1;
			ro = axisToSplit.split(nChunks);
                        
                        LOGGER.info("Total number of chunks: " + nChunks);
		}
                
                String level0_table = schema + "." + indexPrefix + "level0";
                
                // ensure level0 table namespace is available
                if (SqlUtils.dbType(c) == DbType.MONETDB) {
                    if (SqlUtils.existsTable(c, schema, indexPrefix + "level0")) {
                        sql_build.add("DROP TABLE " + level0_table + ";\n");
                    }
                } else {
                    sql_build.add("DROP TABLE IF EXISTS " + level0_table + ";\n");
                }
                sql_build.newLine();
                
                // ensure the chunks table does not exist already
                String chunks_table = table_pa + "_chunks_tmp";
                if (SqlUtils.dbType(c) == DbType.MONETDB) {
                    if (SqlUtils.existsTable(c, schema, chunks_table)) {
                        sql_build.add("DROP TABLE " + chunks_table + ";\n");
                    }
                } else {
                    sql_build.add("DROP TABLE IF EXISTS " + chunks_table + ";\n");
                }
                sql_build.newLine();
                
                // create the chunks table
                // this is a temporary table which holds all the data of the chunks
                // and later get GROUP'ed into the final PA table
                StringBuilder createSql = new StringBuilder("CREATE TABLE ");
                createSql.append(chunks_table).append(" (\n");
                createSql.append("ckey ").append(kd.keySqlType()).append(",\n");
                if ((aggregateMask&AGGR_COUNT) != 0) createSql.append("\t").append("countAggr bigint,\n");
                if ((aggregateMask&AGGR_SUM) != 0) createSql.append("\t").append("sumAggr ").append(aggregateType).append(",\n");
                if ((aggregateMask&AGGR_MIN) != 0) createSql.append("\t").append("minAggr ").append(aggregateType).append(",\n");
                if ((aggregateMask&AGGR_MAX) != 0) createSql.append("\t").append("maxAggr ").append(aggregateType).append("\n");
                
                // ensure that column list does not end with a comma (,)
                if (createSql.charAt(createSql.length()-2) == ',') {
                    createSql.deleteCharAt(createSql.length()-2);
                }
                
                createSql.append(");\n");
                
                sql_build.add(createSql.toString());
                
		for (i = 0; i < nChunks; i++) {
                        long chunkStartTime = System.currentTimeMillis();
                    
			/*
			 * Generate the level 0 table
			 */
			String where = null;
			if ( nChunks > 1 ) {
				where = "("+axisToSplit.columnExpression()+">="+SqlUtils.gen_Constant(c,ro[i][0])+" AND "+axisToSplit.columnExpression()+"<"+SqlUtils.gen_Constant(c,ro[i][1])+")";
				sql_build.add("-- adding increment "+i+" to pa index\n");
                                LOGGER.info("Generating increment " + (i+1) + " out of " + nChunks);
			} else {
				sql_build.add("-- computing pa_index in one step\n");
                                LOGGER.info("Generating full PA index in one step");
			}
			                        
			//
			sql_build.add(generate_level0(c, level0_table,
					schema + "." + table, where, axis, aggregateColumn,
					aggregateMask));
			sql_build.newLine();
                                                
			String level0 = level0_table;

			/*
			 * Now generate the index levels 0 + n-1
			 */                                                                        
			if ( !gen_optimized ) {
				String level0_n = generate_level0_n(c, chunks_table, level0,
						axis, dimTable, genKey, aggregateMask);
				sql_build.add(level0_n);
				sql_build.newLine();
			} else {
				// use the optimized strategy
				generate_optimized(c, sql_build, chunks_table, level0, lfp_table, axis, genKey, aggregateMask);
			}
                        
                        sql_build.add("DROP TABLE " + level0_table + ";\n");
                                                
			if ( false ) {
				if ( gen_optimized )
					sql_build.add(SqlUtils.gen_Select_INTO(c, 
							"pa_opt", "SELECT *", "FROM " + table_pa, false));
				else
					sql_build.add(SqlUtils.gen_Select_INTO(c, 
							"pa_org", "SELECT *", "FROM " + table_pa, false));
			}
                        
                        long chunkExecTime = System.currentTimeMillis() - chunkStartTime;
                        if (nChunks > 1) {
                            LOGGER.info("Finished increment in " + chunkExecTime + " ms");
                        } else {
                            LOGGER.info("Finished generating full PA index");
                        }
		}
                
                
                sql_build.newLine();

                // Group chunks by ckey and move into final PA table
                // MonetDB also requires that the PA table is sorted on the primary key (ckey)
                // to achieve high-performance (see: https://www.monetdb.org/pipermail/users-list/2014-July/007400.html)
                StringBuilder insertSql = new StringBuilder("INSERT INTO ");
                insertSql.append(table_pa).append(" ");
                insertSql.append("SELECT ckey, ");
                
                if ((aggregateMask & AGGR_COUNT) !=0) insertSql.append("SUM(countaggr) AS countaggr, ");
                if ((aggregateMask & AGGR_SUM) !=0) insertSql.append("SUM(sumaggr) AS sumaggr, ");
                if ((aggregateMask & AGGR_MIN) !=0) insertSql.append("MIN(minaggr) AS minaggr, ");
                if ((aggregateMask & AGGR_MAX) !=0) insertSql.append("MAX(maxaggr) AS maxaggr");

                // ensure that column list does not end with a comma (,)
                if (insertSql.charAt(insertSql.length()-2) == ',') {
                    insertSql.deleteCharAt(insertSql.length()-2);
                }
                
                insertSql.append(" FROM ").append(chunks_table);
                insertSql.append(" GROUP BY ckey \n");
                insertSql.append(" ORDER BY ckey ASC;\n");
                
                sql_build.add(insertSql.toString());
                sql_build.newLine();

                sql_build.add("DROP TABLE " + chunks_table + ";\n");
                
                sql_build.newLine();                
 
		if (sql_build.executeDirectly() == false) {
			System.out.println("\n#! SCRIPT:\n"+sql_build.getScript());
			System.out.flush(); // flush before possible crash
			PrintWriter writer;
			try {
				writer = new PrintWriter("script.sql", "UTF-8");
				writer.println(sql_build.getScript());
				writer.flush();
				writer.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		sql_build.executeBatch();
		create_time_ms = new Date().getTime() - create_time_ms;

		if (showAxisAndKey) {
			long input_tuples = SqlUtils.count(c, schema, table, "*");
			long cells = SqlUtils.count(c, schema, table + PA_EXTENSION, "*");
			int perc = (int)(((double)cells/(double)input_tuples)*100);
			LOGGER.debug("# input tuples=" + input_tuples + ", aggregate index cells=" + cells + "["+perc+"%]");
			LOGGER.debug("# aggregate index creation time = "
					+ create_time_ms + "ms");
		}

		// add the index to the repository
		update_repository(c, schema, table, label, aggregateColumn, aggregateType, kd, axis, aggregateMask);

		/*
		 * run the constructor initialization code for the PreAggregate Object
		 */
		_init(c, schema, table, label);
	}
        
        protected String create_index_table (SqlScriptBuilder sql_build, String override_name, AggrKeyDescriptor kd) throws SQLException {
            /*
            * Generate the table which contains the final index
            */
           String table_pa; 
           String table_pa_idx;
           if ( override_name == null ) {
                   table_pa = schema + "." + table + PA_EXTENSION;
                   table_pa_idx = table + PA_EXTENSION + "_idx";
           } else {
                   table_pa = schema + "." + override_name;
                   table_pa_idx = override_name + "_idx";
           }
           sql_build.add("-- create the table containg the pre aggregate index\n");

           if (SqlUtils.dbType(c) == DbType.MONETDB) {
               if (SqlUtils.existsTable(c, schema, table_pa)) {
                   sql_build.add("DROP TABLE " + table_pa + ";\n");
               }
           } else {                
               sql_build.add("DROP TABLE IF EXISTS " + table_pa + ";\n");
           }

           StringBuilder createSql = new StringBuilder("CREATE TABLE ");
           createSql.append(table_pa).append(" (\n");
           createSql.append("\t").append("ckey ").append(kd.keySqlType()).append(" NOT NULL PRIMARY KEY, \n");
           
           if ((aggregateMask & AGGR_COUNT) !=0) createSql.append("\t").append("countAggr bigint,\n");
           if ((aggregateMask & AGGR_SUM) !=0) createSql.append("\t").append("sumAggr ").append(aggregateType).append(",\n");
           if ((aggregateMask & AGGR_MIN) !=0) createSql.append("\t").append("minAggr ").append(aggregateType).append(",\n");
           if ((aggregateMask & AGGR_MAX) !=0) createSql.append("\t").append("maxAggr ").append(aggregateType).append("\n");
           
           // ensure that column list does not end with a comma (,)
           if (createSql.charAt(createSql.length()-2) == ',') {
               createSql.deleteCharAt(createSql.length()-2);
           }
           
           createSql.append(");\n");
           
           sql_build.add(createSql.toString());

           if (SqlUtils.dbType(c) == DbType.MONETDB) {
               sql_build.add("CREATE INDEX " + table_pa_idx + " ON " + table_pa + " (ckey);\n");
           } else {                
               sql_build.add("CREATE INDEX " + table_pa_idx + " ON " + table_pa + " USING HASH(ckey);\n");
           }
           sql_build.newLine();
           
           return table_pa;
        }

	protected void gen_lfp_table(Connection c, SqlScriptBuilder sql_build , String lfp_table, AggregateAxis axis[], String dimTable[]) throws SQLException {
		int i, j;
		
		String lfp_table_tmp = lfp_table + "_tmp";
		StringBuilder with = new StringBuilder();
		StringBuilder select = new StringBuilder();
                
                if (SqlUtils.existsTable(c, schema, lfp_table)) {
                    sql_build.add("DROP TABLE " + lfp_table + ";");
                    sql_build.newLine();
                }
                		
                if (SqlUtils.dbType(c) == DbType.MONETDB) {
                    // MonetDB does not support WITH recursive
                    // therefore we need to create the temporary level/factor tables manually
                    for(i=0; i<axis.length; i++) {
                        if (SqlUtils.existsTable(c, schema, "t" + i)) {
                            sql_build.add("DROP TABLE " + schema + ".t" + i + ";");
                            sql_build.newLine();
                        }
                        
                        sql_build.add("CREATE TABLE " + schema + ".t" + i + " (level int, factor int);");
                        sql_build.newLine();
                        
                        for(int level=0; level <= axis[i].maxLevels(); level++) {
                            int factor = 1 * (int)Math.pow(axis[i].N(), level);
                            sql_build.add("INSERT INTO " + schema + ".t" + i + " VALUES (" + level + ", " + factor + ");");
                            sql_build.newLine();
                        }      
                        
                        sql_build.newLine();
                    }
                } else {
                    with.append("WITH RECURSIVE\n");
                    for(i=0; i<axis.length; i++) {
                            if ( i > 0 )
                                    with.append(",\n");
                            with.append("\t t"+i+"(level,factor) AS (\n");
                            with.append("\t\tVALUES(0,1)\n");
                            with.append("\tUNION ALL\n");
                            with.append("\t\tSELECT level+1, factor*"+axis[i].N() + " FROM t"+i+" WHERE level < "+axis[i].maxLevels()+"\n");
                            with.append("\t)");
                    }
                    with.append("\n");
                }
		
		StringBuilder greatest = new StringBuilder();
		greatest.append("greatest(");
		for(i=0; i<axis.length; i++) {
			if ( i>0 )
				greatest.append(",");
			greatest.append("t"+i+".level");
		}
		greatest.append(")");
		for(i=0; i<axis.length; i++) {
			if ( i > 0 )
				select.append(",\n");
			select.append("\tt"+i+".level as target_l"+i+", least(t"+i+".level, "+greatest+"-1) AS source_l"+i);
			select.append(", div(dd"+i+".factor,sd"+i+".factor) as factor_f"+i);
		}
		
		StringBuilder from = new StringBuilder();
		from.append("FROM\t");
		for(i=0; i<axis.length; i++) {
			if ( i>0 )
				from.append(", ");
			from.append("t"+i+" AS t"+i);
		}
		from.append(",\n\t");
		for(i=0; i<axis.length; i++) {
			if ( i>0 )
				from.append(", ");
			from.append(dimTable[i]+" AS sd"+i);
		}
		from.append(",\n\t");
		for(i=0; i<axis.length; i++) {
			if ( i>0 )
				from.append(", ");
			from.append(dimTable[i]+" AS dd"+i);
		}
		from.append("\n");
		
		StringBuilder where = new StringBuilder();
		where.append("WHERE\t");
		where.append("(");
		for(i=0; i<axis.length; i++) {
			if ( i>0 )
				where.append(" or ");
			where.append("t"+i+".level>0");
		}
		where.append(")\n\t");
		for(i=0; i<axis.length; i++) {
			where.append("and "+ "dd"+i+".level=t"+i+".level ");
		}
		where.append("\n");
		for(i=0; i<axis.length; i++) {
			where.append("\tand "+"sd"+i+".level = least(t"+i+".level, "+greatest+"-1)\n");
		}
                                
		String lfp_tmp = SqlUtils.gen_Select_INTO(c, 
				lfp_table_tmp,
				"SELECT" + select,
				from.toString() + 
				where,
				false);
                
                if (SqlUtils.dbType(c) == DbType.MONETDB) {
                    if (SqlUtils.existsTable(c, schema, lfp_table_tmp)) {
                        sql_build.add("DROP TABLE " + lfp_table_tmp + ";\n");
                    }
                } else {                
                    sql_build.add("DROP TABLE IF EXISTS " + lfp_table_tmp + ";\n");
                }
                
		sql_build.add(with.toString() + lfp_tmp + "\n");
		
		select = new StringBuilder();
		select.append("SELECT\t");
		for(i=0; i<axis.length; i++) {
			select.append("target_l"+i+",\n\t");
			StringBuilder diff = new StringBuilder();
			if ( i > 0 ) {
				for(j=0; j<i; j++) {
					if ( j>0 )
						diff.append("+");
					diff.append("target_l"+j+"-source_l"+j);
				}
			}
			if ( i == 0 )
				select.append("source_l"+i+",\n\t");
			else {
				select.append("case ");
				select.append(diff.toString());
				select.append(" when 0 then source_l"+i+" else target_l"+i);
				select.append(" end AS source_l"+i+",\n\t");
			}
			if ( i == 0 )
				select.append("factor_f"+i+"");
			else {
				select.append("case ");
				select.append(diff.toString());
				select.append(" when 0 then factor_f"+i+" else 1");
				select.append(" end AS factor_f"+i+"");
			}
			if ( i < (axis.length - 1) )
				select.append(",\n\t");
		}
		String lfp = SqlUtils.gen_Select_INTO(c, 
				lfp_table,
				select.toString(),
				"FROM " + lfp_table_tmp,
				false);
		sql_build.add(lfp);
                
                sql_build.newLine();
                
		sql_build.add("DROP TABLE " + lfp_table_tmp + ";\n");
                
                if (SqlUtils.dbType(c) == DbType.MONETDB) {
                    for(i=0; i<axis.length; i++) {
                        sql_build.add("DROP TABLE " + schema + ".t" + i + ";");
                        sql_build.newLine();
                    }
                }
                
                sql_build.newLine();
                
                sql_build.addPost("DROP TABLE " + lfp_table + ";\n");
	}
        
        public String select_level0 (Connection c, String from, String where, AggregateAxis axis[], String aggregateColumn, int aggregateMask) throws SQLException {
            StringBuilder select = new StringBuilder("SELECT ");
            StringBuilder gb	 = new StringBuilder();
       
            for(int i=0; i < axis.length; i++) {
                if (i>0) {
                        select.append(",\n\t");
                        gb.append(',');
                }
                if ( gen_optimized ) {
                        select.append("0 AS l").append(i).append(",\n\t");
                }

                if (SqlUtils.dbType(c) == DbType.MONETDB) {
                    select.append("CAST(").append(rangeFunName(i)).append("(").append(axis[i].columnExpression()).append(") AS integer) AS i").append(i);
                } else {                        
                    select.append(rangeFunName(i)).append("(").append(axis[i].columnExpression()).append(") :: integer AS i").append(i);
                }

                gb.append("i").append(i);
            }
            
            if ((aggregateMask & AGGR_COUNT) != 0) select.append(",\n\tCOUNT(").append(aggregateColumn).append(") AS countAggr");            
            if ((aggregateMask &AGGR_SUM) != 0) select.append(",\n\tSUM(").append(aggregateColumn).append(") AS sumAggr"); 
            if ((aggregateMask &AGGR_MIN) != 0) select.append(",\n\tMIN(").append(aggregateColumn).append(") AS minAggr"); 
            if ((aggregateMask &AGGR_MAX) != 0) select.append(",\n\tMAX(").append(aggregateColumn).append(") AS maxAggr"); 
            
            select.append(" FROM ").append(from);
            
            if ( where != null ) {
                select.append(" WHERE ").append(where);
            }
            
            select.append("\nGROUP BY ").append(gb);
            select.append("\n");
            
            return select.toString();
        }
	
	public String generate_level0(Connection c, String level0_table, String from, String where, AggregateAxis axis[], String aggregateColumn, int aggregateMask) 
	throws SQLException {
		
		String level0 = SqlUtils.gen_Create_Table_As_Select(
                                c, 
				level0_table,
                                select_level0(c, from, where, axis, aggregateColumn, aggregateMask),
				false);
		return level0;
	}
	
	protected void generate_optimized(Connection c, SqlScriptBuilder sql_build, String pa_tmp_table, String level0, String lfp_table, AggregateAxis axis[],
			String genKey, int aggregateMask) throws SQLException {
		// first compute the total number of levels
		int i, sumlevels = 0;
		for(i=0; i<axis.length; i++)
			sumlevels += axis[i].maxLevels();
		for(int thislevel=1; thislevel<=sumlevels; thislevel++) {
			StringBuilder select = new StringBuilder();
			StringBuilder where = new StringBuilder();
			StringBuilder gb = new StringBuilder();
			
			select.append("SELECT");
			for(i=0; i<axis.length; i++) {
				select.append("\ttarget_l"+i+" AS "+ "l"+i+", DIV(level0.i"+i+",factor_f"+i+") as ii"+i);
				if ( i!=axis.length-1 )
					select.append(",\n");
			}
			if ((aggregateMask & AGGR_COUNT) != 0)
				select.append(",\n\tSUM(level0.countAggr) AS countAggr");
			if ((aggregateMask & AGGR_SUM) != 0)
				select.append(",\n\tSUM(level0.sumAggr) AS sumAggr");
			if ((aggregateMask & AGGR_MIN) != 0)
				select.append(",\n\tMIN(level0.minAggr) AS minAggr");
			if ((aggregateMask & AGGR_MAX) != 0)
				select.append(",\n\tMAX(level0.maxAggr) AS maxAggr");
			//
			where.append("WHERE");
			StringBuilder lsum = new StringBuilder();
			for(i=0; i<axis.length; i++) {
				where.append("\tlevel0.l"+i+"=source_l"+i+" and\n");
				if ( i>0 )
					lsum.append("+");
				lsum.append("target_l"+i);
			}
			where.append("\t("+lsum+")="+thislevel+"\n");
			//
			gb.append("GROUP BY");
			for(i=0; i<axis.length; i++) {
				if ( i >0 )
					gb.append(",");
				gb.append(" target_l"+i+", ii"+i+", factor_f"+i);
			}
			
			String stat = "INSERT INTO "+level0+"\n"+
			   select +
			   "\nFROM " + lfp_table + ", " + level0 + " AS level0\n" +
			   where +
			   gb + ";";
			
			sql_build.add(stat);
			sql_build.newLine();
			sql_build.newLine();
		}
		StringBuilder keystat = new StringBuilder();
		keystat.append("SELECT\t"+genKey+"(");
		for(i=0;i<axis.length; i++) {
			if ( i>0 )
				keystat.append(",");
			keystat.append("l"+i+",i"+i);
			// keystat.append("l"+i+",i"+i+"+1");
		}
		keystat.append(") as ckey");
		if ((aggregateMask & AGGR_COUNT) != 0)
			keystat.append(",\n\tcountAggr");
		if ((aggregateMask & AGGR_SUM) != 0)
			keystat.append(",\n\tsumAggr");
		if ((aggregateMask & AGGR_MIN) != 0)
			keystat.append(",\n\tminAggr");
		if ((aggregateMask & AGGR_MAX) != 0)
			keystat.append(",\n\tmaxAggr");
			
                
                sql_build.add("INSERT INTO " + pa_tmp_table + " " + keystat.toString() + " FROM " + level0 + ";\n");
                
                /*
		sql_build.add(
				SqlUtils.gen_Select_INTO(c, 
				delta_table,
				keystat.toString(),
				"FROM " + level0,
				false)
		);
                */
                
		sql_build.newLine();
	}
	
	public String generate_level0_n(Connection c, String chunks_table,
			String level0, AggregateAxis axis[], String dimTable[],
			String genKey, int aggregateMask) throws SQLException {
		int i;
		StringBuilder select = new StringBuilder();
		StringBuilder from = new StringBuilder();
		StringBuilder where = new StringBuilder();
		StringBuilder gb = new StringBuilder();
		StringBuilder gk = new StringBuilder();
		gk.append(genKey + "(");
		for (i = 0; i < axis.length; i++) {
			if (i > 0) {
				select.append(',');
				from.append(',');
				gb.append(',');
				gk.append(',');
			}
			if (i > 1)
				where.append(" AND ");
			select.append("\n\t\tdim" + i + ".level" + " AS l" + i);
			select.append(",\n\t\t"
					+ SqlUtils.gen_CAST(c, SqlUtils.gen_DIV(c, "level0.i" + i, "dim" + i + ".factor"),"integer")
					+ " AS v" + i);
			from.append("\n\t\t" + dimTable[i] + " AS dim" + i);
			gk.append("l" + i + "," + "v" + i);
			if (i > 0)
				where.append("dim0.level=dim" + i + ".level");
			gb.append("l" + i + ",v" + i);
		}
		gk.append(")");
		if ((aggregateMask & AGGR_COUNT) != 0)
			select.append(",\n\t\tSUM(level0.countAggr) AS countAggr");
		if ((aggregateMask & AGGR_SUM) != 0)
			select.append(",\n\t\tSUM(level0.sumAggr) AS sumAggr");
		if ((aggregateMask & AGGR_MIN) != 0)
			select.append(",\n\t\tMIN(level0.minAggr) AS minAggr");
		if ((aggregateMask & AGGR_MAX) != 0)
			select.append(",\n\t\tMAX(level0.maxAggr) AS maxAggr");
		String DATA = level0 + " AS level0";
		from.append(",\n\t\t" + DATA);

		String subindexQ = "SELECT " + select + "\n\t FROM\t" + from
		+ "\n\tGROUP BY " + gb;

		String aggrAttr = ((aggregateMask & AGGR_COUNT) != 0 ? ",\n\tcountAggr"
				: "")
				+ ((aggregateMask & AGGR_SUM) != 0 ? ",\n\tsumAggr" : "")
				+ ((aggregateMask & AGGR_MIN) != 0 ? ",\n\tminAggr" : "")
				+ ((aggregateMask & AGGR_MAX) != 0 ? ",\n\tmaxAggr" : "");

		// first update, then insert to prevent problems
                StringBuilder insert = new StringBuilder("INSERT INTO ");
                insert.append(chunks_table).append(" SELECT ").append(gk).append(" AS ckey").append(aggrAttr);
                insert.append(" FROM (").append(subindexQ).append(") AS siq");

		return insert.toString();
	}

	/*
	 * 
	 * The Query section
	 * 
	 */
	public ResultSet SQLquery_interval(int queryAggregateMask, Object obj_range_interval[][][]) throws SQLException {
		ResultSet result = null;

		StringBuilder qb = new StringBuilder();

		for(int i=0; i<obj_range_interval.length; i++) {
			Object obj_range[][] = obj_range_interval[i];

			if ( i > 0 )
				qb.append("UNION\n");
			qb.append("(");
			qb.append(SQLquery_string(queryAggregateMask,obj_range,i + " AS i, "));
			qb.append(")\n");
		}
		String sql = qb + ";";
		System.out.println("# interval query=\n" + sql);
		result = SqlUtils.execute(c, sql);
		return result;
	}

	public ResultSet SQLquery_grid(int queryAggregateMask, Object iv_first_obj[][], int iv_count[]) throws SQLException {
		ResultSet result = null;
		int i;

		boolean allowOutsideRange = true;
		if ( iv_first_obj.length != axis.length )
			throw new SQLException("SQLquery_grid: dimension of first interval does not meet axis");
		if ( iv_count.length != axis.length )
			throw new SQLException("SQLquery_grid: dimension of interval count does not meet axis");
		int iv_size[] = new int[axis.length];
		int iv_first[] = new int[axis.length];
		int iv_last[] = new int[axis.length];
		for(i=0; i<axis.length; i++) {
			if (axis[i].isMetric()) {
				MetricAxis ax = (MetricAxis) axis[i];

				if (false) {
					if (!ax.exactIndex(iv_first_obj[i][RMIN]))
						throw new SQLException(
								"SQLquery_grid: start of first interval in dim "
										+ i + " not on boundary: "
										+ iv_first_obj[i][RMIN]);
					if (!ax.exactIndex(iv_first_obj[i][RMAX]))
						throw new SQLException(
								"SQLquery_grid: end of first interval in dim "
										+ i + " not on boundary: "
										+ iv_first_obj[i][RMAX]);
				}
				// TODO: check out of range values
				if (allowOutsideRange) {
					iv_first[i] = ax.getIndex(iv_first_obj[i][RMIN], false);
					iv_last[i] = ax.getIndex(iv_first_obj[i][RMAX], false);
					iv_size[i] = iv_last[i] - iv_first[i];
				} else {
					iv_first[i] = ax.getIndex(iv_first_obj[i][RMIN], true);
					iv_last[i] = ax.getIndex(iv_first_obj[i][RMAX], true);
					if (iv_first[i] >= iv_last[i])
						throw new SQLException(
								"grid range first > last on dimension " + i);
					if (iv_first[i] < 0 || iv_last[i] < 0)
						throw new SQLException(
								"grid range outside pre-aggregate range on dimension "
										+ i);
					iv_size[i] = iv_last[i] - iv_first[i];
				}
			} else {
				// it must be Nominal
				NominalAxis nax = (NominalAxis)axis[i];
                                
                                if (iv_first_obj[i][RMIN] instanceof Integer && iv_first_obj[i][RMAX] instanceof Integer) {
                                    iv_first[i] = ((Integer)iv_first_obj[i][RMIN]).intValue();
                                                                        
                                    if (iv_first[i] != ((Integer)iv_first_obj[i][RMAX]).intValue()) {
                                        throw new SQLException("Nominal Integer bounds must be equal on dimension " + i);
                                    }
                                } else if (iv_first_obj[i][RMIN] instanceof String && iv_first_obj[i][RMAX] instanceof String) {
                                    // check if NominalAxis has word list and is therefore able to convert String bounds into Integer index bounds
                                    if (nax.fromFIELDstore().equals(NominalAxis.WORDLISTNOMINAL) == false) {
                                        throw new SQLException("String bounds specified on dimension " + i + " but no WordList available");
                                    }
                                    
                                    iv_first[i] = nax.getWordIndex((String)iv_first_obj[i][RMIN]);
                                    
                                    if ( iv_first[i] != nax.getWordIndex((String)iv_first_obj[i][RMAX]) ) {
					throw new SQLException("Nominal String bounds must be equal on dimension " + i);
                                    }
                                } else {
                                    throw new SQLException("Bad bounds on dimension " + i + ", must be either Integer or String");
                                }
                                
				if ( iv_first[i] < 0 )
					throw new SQLException(
							"unknown word "+iv_first_obj[i][RMIN]+" dimension " + i);
				
				iv_last[i] = iv_first[i] + 1;
				iv_size[i] = 1;
			}
		}
		// now have all the info we need
		// what do do in the sql case ?
		if ( serversideStairwalk ) {
			int swgc[][] = new int[axis.length][3]; // start/width/gridcells

			
			long prev_dimsize = 1;
			for(i=0; i<axis.length; i++) {
				swgc[i][0] = iv_first[i];	// start
				swgc[i][1] = iv_size[i];	// width
				swgc[i][2] = iv_count[i];	// gridcells
				if ( i == 0 ) {
				} else {
				}
				prev_dimsize *= iv_count[i];
			} 
			StringBuilder sqlaggr = new StringBuilder();
                        
                        if (SqlUtils.dbType(c) == DbType.MONETDB) {
                            if ((queryAggregateMask & aggregateMask & AGGR_COUNT) != 0)
                                    sqlaggr.append(",sum(aggr1) AS countAggr");
                            if ((queryAggregateMask & aggregateMask & AGGR_SUM) != 0)
                                    sqlaggr.append(",sum(aggr2) AS sumAggr");
                            if ((queryAggregateMask & aggregateMask & AGGR_MIN) != 0)
                                    sqlaggr.append(",min(aggr3) AS minAggr");
                            if ((queryAggregateMask & aggregateMask & AGGR_MAX) != 0)
                                    sqlaggr.append(",max(aggr4) AS maxAggr");
                        } else {                        
                            if ((queryAggregateMask & aggregateMask & AGGR_COUNT) != 0)
                                    sqlaggr.append(",sum(countAggr) AS countAggr");
                            if ((queryAggregateMask & aggregateMask & AGGR_SUM) != 0)
                                    sqlaggr.append(",sum(sumAggr) AS sumAggr");
                            if ((queryAggregateMask & aggregateMask & AGGR_MIN) != 0)
                                    sqlaggr.append(",min(minAggr) AS minAggr");
                            if ((queryAggregateMask & aggregateMask & AGGR_MAX) != 0)
                                    sqlaggr.append(",max(maxAggr) AS maxAggr");
                        }
                        
                        StringBuilder gcells = new StringBuilder("pa_grid");
                        
                        // use enhanced version with MonetDB
                        if (SqlUtils.dbType(c) == DbType.MONETDB) {
                            gcells.append("_enhanced");
                        }
                        
                        gcells.append("(\'").append(grid_paGridQuery(swgc)).append("\'");
                        
                        // MonetDB has a more advanced version of the pa_grid function
                        // so we need to add additional parameters
                        if (SqlUtils.dbType(c) == DbType.MONETDB) {
                            gcells.append(", ").append(SqlUtils.quoteValue(c, schema));
                            gcells.append(", ").append(SqlUtils.quoteValue(c, table + PA_EXTENSION));
                            gcells.append(", ").append(SqlUtils.quoteValue(c, "ckey"));
                            
                            if ((queryAggregateMask & aggregateMask & AGGR_COUNT) != 0)
                                gcells.append(", ").append(SqlUtils.quoteValue(c, "countaggr"));
                            if ((queryAggregateMask & aggregateMask & AGGR_SUM) != 0)
                                gcells.append(", ").append(SqlUtils.quoteValue(c, "sumaggr"));
                            if ((queryAggregateMask & aggregateMask & AGGR_MIN) != 0)
                                gcells.append(", ").append(SqlUtils.quoteValue(c, "minaggr"));
                            if ((queryAggregateMask & aggregateMask & AGGR_MAX) != 0)
                                gcells.append(", ").append(SqlUtils.quoteValue(c, "maxaggr"));
                        }
                        
                        gcells.append(")");
                        
                        
			StringBuilder gk_ex = new StringBuilder();
			StringBuilder order  = new StringBuilder();
                        
			gk_ex.append("gkey");
			int prevBits = 0;
			for(i=0; i<iv_count.length; i++) {
				int dimBits = MetricAxis.log2(iv_count[i]);
				String base = null;
				
				if (prevBits == 0 ) {
					base = "gkey";
				} else {
					base = SqlUtils.gen_DIV(c, "gkey", ""+MetricAxis.pow2(prevBits));
				}
				gk_ex.append("," + SqlUtils.gen_MOD(c, base, ""+MetricAxis.pow2(dimBits)) + " AS d"+i);
				if ( i > 0 )
					order.append(",");
				order.append("d"+i);
				prevBits += dimBits;
			}
                        
                        StringBuilder sql = new StringBuilder("SELECT ");
                        sql.append(gk_ex).append(sqlaggr);
                        sql.append(" FROM ");
                        if (SqlUtils.dbType(c) == DbType.MONETDB) {
                            // only select from pa_grid function
                           sql.append(gcells);
                        } else {
                            // select from _pa table and pa_grid function and join on ckey/pakey
                            sql.append(schema).append(".").append(table).append(PA_EXTENSION).append(", ").append(gcells);
                            
                            if (kd.kind() == AggrKeyDescriptor.KD_BYTE_STRING) {
                                sql.append(" WHERE ckey = pa_bytekey");
                            } else {
                                sql.append(" WHERE ckey = pakey");
                            }
                        }
                        sql.append(" GROUP BY gkey");
                        sql.append(" ORDER BY ").append(order);
                        
			System.out.println("#!GRID_QUERY="+sql.toString());	
			//
			if ( false ) {
				int cellnr[] = { 7, 8 };
				
				System.out.println("#!DOING A RECHECK!!");
				Object cellrange[][] = new Object[axis.length][2];
				for(i=0; i<axis.length; i++) {
					int iv_from = iv_first[i] + cellnr[i]*iv_size[i];
					int iv_to   = iv_first[i] + (cellnr[i] + 1) * iv_size[i] + 1;
					cellrange[i][0] = axis[i].reverseValue(iv_from);
					cellrange[i][1] = axis[i].reverseValue(iv_to);
				}
				query("count", cellrange);
			}
			
			if (false) {
				System.out.println("\n#!MANUAL COMPUTED GRID RESULTS");
				for (int d0 = 0; d0 < iv_count[0]; d0++) {
					for (int d1 = 0; d1 < iv_count[1]; d1++) {

						int cellnr[] = { d0, d1 };

						Object cellrange[][] = new Object[axis.length][2];
						for (i = 0; i < axis.length; i++) {
							int iv_from = iv_first[i] + cellnr[i] * iv_size[i];
							int iv_to = iv_first[i] + (cellnr[i] + 1)
									* iv_size[i] + 1;
							cellrange[i][0] = axis[i].reverseValue(iv_from);
							cellrange[i][1] = axis[i].reverseValue(iv_to);
						}
						if (false) {
							ResultSet rs = SQLquery(queryAggregateMask,
									cellrange);
							int res = 0;
							if (rs.next())
								res = rs.getInt(1);
							if (res > 0)
								System.out.println("GRID[" + d0 + "," + d1
										+ "] = " + res);
						} else {
							query("count", cellrange);
						}
					}
				}
			}
			
			//
			result = SqlUtils.execute(c,sql.toString());
		} else {
			// explode it
			PermutationGenerator p = new PermutationGenerator(axis.length);
			for(i=0; i<axis.length; i++) {
				p.setRange(i,0,iv_count[i]);
			}
			int		ranges[][] = new int[axis.length][2];
			StringBuilder qb = new StringBuilder();
			p.start();
			while ( p.next() ) {
				StringBuilder ib = new StringBuilder();

				for(i=0; i<axis.length; i++) {
					ranges[i][RMIN] = iv_first[i] + iv_size[i] * (p.permutation[i] + 0);
					ranges[i][RMAX] = iv_first[i] + iv_size[i] * (p.permutation[i] + 1);

					ib.append( p.permutation[i] + " AS d"+i+", ");
				}
				if ( qb.length() > 0 )
					qb.append("UNION\n");
				qb.append( SQLquery_string(queryAggregateMask,ib.toString(),ranges) + "\n");
				// incomplete
			}
			qb.append(';');
			System.out.println("#! executing Grid Query:\n"+qb);
			result = SqlUtils.execute(c, qb.toString());
		}
		return result;
	}

	public ResultSet SQLquery_grid_standard(int queryAggregateMask, Object iv_first_obj[][], int iv_count[]) throws SQLException {
		ResultSet result = null;
		int i;

		if ( iv_first_obj.length != axis.length )
			throw new SQLException("SQLquery_grid: dimension of first interval does not meet axis");
		if ( iv_count.length != axis.length )
			throw new SQLException("SQLquery_grid: dimension of interval count does not meet axis");
		int iv_size[] = new int[axis.length];
		int iv_first[] = new int[axis.length];
		// now have all the info we need
		// what do do in the sql case ?
		StringBuilder sqlaggr = new StringBuilder();
		if ((queryAggregateMask & aggregateMask & AGGR_COUNT) != 0)
			sqlaggr.append(",count(*) AS countAggr");
		if ((queryAggregateMask & aggregateMask & AGGR_SUM) != 0)
			sqlaggr.append(",sum("+aggregateColumn+") AS sumAggr");
		if ((queryAggregateMask & aggregateMask & AGGR_MIN) != 0)
			sqlaggr.append(",min("+aggregateColumn+") AS minAggr");
		if ((queryAggregateMask & aggregateMask & AGGR_MAX) != 0)
			sqlaggr.append(",max("+aggregateColumn+") AS maxAggr");

                StringBuilder cols = new StringBuilder();
		StringBuilder sqlgkey = new StringBuilder();
		StringBuilder sqlwhere = new StringBuilder();
		StringBuilder sqlgroupby = new StringBuilder();
                
		int factor = 1;
		for(i=0; i < axis.length; i++) {
                    String colKey = "d" + i;

                    if (axis[i] instanceof MetricAxis) {
                        MetricAxis metricAxis = (MetricAxis) axis[i];
                        AxisIndexer indexer = metricAxis.getIndexer();

                        // normal number-based axis?
                        if (indexer instanceof DoubleAxisIndexer || indexer instanceof LongAxisIndexer || indexer instanceof IntegerAxisIndexer) {
                            // coordinates	
                            double start = (Double) iv_first_obj[i][0];
                            double end = (((Double)iv_first_obj[i][0])+iv_count[i]*(((Double)iv_first_obj[i][1])-((Double)iv_first_obj[i][0])));

                            // calculate range between start and end coordinate
                            double range = (end - start);

                            // extend range with BASEBLOCKSIZE
                            // so that the end coordinates falls within range
                            range = range + (Double)metricAxis.BASEBLOCKSIZE();

                            // calculate factor to divide by
                            double divideBy = range / iv_count[i];

                            // calculate index of start coordinate
                            // which is used to compensate calculation of indexes
                            // so that the start index is *always* 0 (zero)
                            double startIndex = start / divideBy;

                            // MonetDB doesn't support expressions in the GROUP BY clause
                            // therefore these must first be separately included as additional columns
                            if (SqlUtils.dbType(c) == DbType.MONETDB) {
                                cols.append(", floor( ("+axis[i].columnExpression()+"/"+ divideBy +")");
                                if (startIndex > 0) {
                                    cols.append(" - ").append(startIndex);
                                } else if (startIndex < 0) {
                                    cols.append(" + ").append(Math.abs(startIndex));
                                }
                                cols.append(") AS " + colKey);
                                sqlgkey.append(colKey + "*" + factor + "+");
                                
                            } else {
                                sqlgkey.append("floor( (").append(axis[i].columnExpression()).append("/").append(divideBy).append(")");
                                if (startIndex > 0) {
                                    sqlgkey.append(" - ").append(startIndex);
                                } else if (startIndex < 0) {
                                    sqlgkey.append(" + ").append(Math.abs(startIndex));
                                }                                    
                                sqlgkey.append(")*").append(factor).append("+");

                                cols.append(", floor( (").append(axis[i].columnExpression()).append("/").append(divideBy).append(")");
                                if (startIndex > 0) {
                                    cols.append(" - ").append(startIndex);
                                } else if (startIndex < 0) {
                                    cols.append(" + ").append(Math.abs(startIndex));
                                }    
                                cols.append(") AS " + colKey);
                                sqlgroupby.append(colKey + ",");
                            }

                            sqlwhere.append( " and "+axis[i].columnExpression()+">="+ start);
                            sqlwhere.append( " and "+ axis[i].columnExpression()+"<="+ end);

                        // time-based axis?
                        } else if (indexer instanceof TimestampAxisIndexer) {                            
                            // time dimension filter
                            sqlwhere.append( " AND " + axis[i].columnExpression() + " >= CAST('" + ((Timestamp)iv_first_obj[i][0]) + "' AS timestamp with time zone)");
                            sqlwhere.append(" AND ").append(axis[i].columnExpression()).append(" <= CAST('")
                                    .append(((Timestamp)iv_first_obj[i][1])).append("' AS timestamp with time zone)");
                            

                            // also need to group by time dimension (i.e. divide result into time buckets)?
                            if(iv_count[i] > 1){
                                    // Dennis Pallett:
                                    // it's likely that the code below is not correct yet, so disable it for now
                                    throw new UnsupportedOperationException("Time dimension does not support groups yet!");
                                    
                                    // TODO: implement grouping for time dimension
                                    
                                    /*
                                    sqlgkey.append("floor(extract('epoch' from "+axis[i].columnExpression()+")/(extract('epoch' from '"+(((Timestamp)iv_first_obj[i][1])+"'::timestamp with time zone) - extract('epoch' from '"+((Timestamp)iv_first_obj[i][0]))+"'::timestamp with time zone)))*"+factor+"+");				
                                    sqlgroupby.append("floor(extract('epoch' from "+axis[i].columnExpression()+")/(extract('epoch' from '"+(((Timestamp)iv_first_obj[i][1])+"'::timestamp with time zone) - extract('epoch' from '"+((Timestamp)iv_first_obj[i][0]))+"'::timestamp with time zone))),");
                                    */
                            } else {
                                cols.append(", '0' AS " + colKey);
                            }
                        } else {
                            throw new UnsupportedOperationException("MetricAxis with indexer of type " + indexer.getClass().getCanonicalName() + " not supported");
                        }
                    } else if (axis[i] instanceof NominalAxis) {
                         // nominal dimension filter
                        NominalAxis nominalAxis = (NominalAxis) axis[i];
                        int wordIndex = -1;
                        
                        if (iv_first_obj[i][0] instanceof Integer) {
                            wordIndex = ((Integer)iv_first_obj[i][0]).intValue();
                        } else if (iv_first_obj[i][0] instanceof String) {
                            // convert actual word to its index
                            wordIndex = nominalAxis.getWordIndex((String)iv_first_obj[i][0]);
                        } else {
                            throw new SQLException("NominalAxis only works with String or Integer objects as start/end");
                        }

                        // add filter to query
                        sqlwhere.append(" AND ").append(nominalAxis.columnExpression()).append(" = ").append(wordIndex);
                        
                        // also need to group by nominal dimension?
                        // currently not supported and not a good idea on how to do this
                        // probably need support for multi-keyword filters first
                        if(iv_count[i] > 1){
                            throw new UnsupportedOperationException("Nominal dimension does not support groups yet!");
                        } else {
                            cols.append(", '0' AS " + colKey);
                        }
                        
                    } else {
                        throw new UnsupportedOperationException("Axis of type " + axis[i].getClass().getCanonicalName() + " not supported");
                    }

                    factor = factor * iv_count[i];
		}
                
                
		String sql = "SELECT " + sqlgkey.toString() + "0 as gkey" + sqlaggr + cols.toString() + 
                             " FROM " + schema + "." + table + 
                             " WHERE true " + sqlwhere.toString()+
                             " GROUP BY "+sqlgroupby.toString();
		sql = sql.substring(0, sql.length()-1);
                System.out.println(sql);
		result = SqlUtils.execute(c, sql);
		//
		return result;
	}

	public ResultSet SQLquery(int queryAggregateMask, Object obj_range[][]) throws SQLException {
		ResultSet result = null;

		String sql = SQLquery_string(queryAggregateMask,obj_range,"") + ";";
		// System.out.println("# main query=\n" + sql);
		result = SqlUtils.execute(c, sql);
		return result;
	}

	private String SQLquery_string(int queryAggregateMask, Object obj_range[][], String extra) throws SQLException {
		int		i;
		int		ranges[][] = new int[axis.length][2];

		if ( obj_range.length != axis.length )
			throw new SQLException("PreAggregate.query(): dimension index and query do not match");
		@SuppressWarnings("unused")
		boolean needsCorrection = false;
		for (i = 0; i < axis.length; i++) {
			if (axis[i].isMetric()) {
				MetricAxis metric = (MetricAxis) axis[i];
				int i_rmin = metric.getIndex(obj_range[i][RMIN], true);
				int i_rmax = metric.getIndex(obj_range[i][RMAX], true);
				ranges[i][RMIN] = i_rmin;
				if ( i_rmin == MetricAxis.INDEX_TOO_SMALL ) {
					i_rmin = 0;
					obj_range[i][RMIN] = metric.low();
				}
				ranges[i][RMAX] = i_rmax;
				if ( i_rmax == MetricAxis.INDEX_TOO_LARGE ) {
					i_rmax = metric.axisSize();
					obj_range[i][RMAX] = metric.high();
				}

				if (metric.exactIndex(obj_range[i][RMAX])) {
					ranges[i][RMAX] -= 1; // adjust upper bound
				}
				if (doResultCorrection
						&& (!metric.exactIndex(obj_range[i][RMIN]) || 
							!metric.exactIndex(obj_range[i][RMAX]))) {
					needsCorrection = true;
				}
			}
		}
		return SQLquery_string(queryAggregateMask,extra,ranges);
	}

	private String SQLquery_string(int queryAggregateMask, String extra_select, int ranges[][]) throws SQLException {
		if ( queryAggregateMask == 0)
			throw new SQLException("pre aggregate query without aggregates");
		else {
			// incomplete, check if the query_aggregates are in the pre_aggregate cells
		}
		// System.out.println("#!AGGR_MASK="+aggregateMask+", QueryMask="+queryAggregateMask);
		StringBuilder b_sqlaggr = new StringBuilder();
		if ((queryAggregateMask&AGGR_COUNT)!=0) b_sqlaggr.append(",SUM(countAggr) AS countAggr");
		if ((queryAggregateMask&AGGR_SUM)!=0) b_sqlaggr.append(",SUM(sumAggr) AS sumAggr");
		if ((queryAggregateMask&AGGR_MIN)!=0) b_sqlaggr.append(",MIN(minAggr) AS minAggr");
		if ((queryAggregateMask&AGGR_MAX)!=0) b_sqlaggr.append(",MAX(maxAggr) AS maxAggr");
		String sqlaggr = b_sqlaggr.substring(1); // rome heading ,

		StringBuffer qb = new StringBuffer("SELECT " + extra_select + sqlaggr + " FROM "+schema+"."+table+PA_EXTENSION);
		qb.append(cellCondition(ranges));
		return qb.toString();
	}

	private String cellCondition(int ranges[][]) throws SQLException {
		if ( serversideStairwalk ) {
			// use Postgres internal pacells2d function
			String pa_grid_str;

			pa_grid_str = "pa_grid_cell('"+range_paGridQuery(ranges)+"') AS pakey "; // 2 times faster
			return ", " + pa_grid_str + " WHERE ckey=pakey";
		} else {
			StringBuilder qb = new StringBuilder();

			Vector<AggrKey> resKeys = computePaCells(kd,ranges,axis);
			System.out.println("#!create WHERE {ckey=v} query: #keys="+resKeys.size());
			qb.append(" WHERE ");
			for (int i = 0; i < resKeys.size(); i++) {
				qb.append(((i > 0) ? " OR " : "") + "ckey="
						+ SqlUtils.quoteValue(c, resKeys.elementAt(i).toKey()));
			}
			return qb.toString();
		}
	}

	public long query(String aggr, Object obj_range[][]) throws SQLException {
		// only for legacy and code examples
		int		i;
		int		ranges[][] = new int[axis.length][2];

		System.out.println("/--------------- query() legacy run ----------------");
		if ( obj_range.length != axis.length )
			throw new SQLException("PreAggregate.query(): dimension index and query do not match (" + axis.length + "<>" + obj_range.length + ")");
		boolean needsCorrection = false;
		for(i=0; i<axis.length; i++) {
			// TODO: do out of range corrections
			
			int rmin = axis[i].getIndex(obj_range[i][RMIN],true);
			ranges[i][RMIN] = rmin;
			int rmax = axis[i].getIndex(obj_range[i][RMAX],true);
			ranges[i][RMAX] = rmax;
			if (axis[i].isMetric()) {
				MetricAxis metric = (MetricAxis)axis[i];
				
				int i_rmin = metric.getIndex(obj_range[i][RMIN], true);
				int i_rmax = metric.getIndex(obj_range[i][RMAX], true);
				ranges[i][RMIN] = i_rmin;
				if ( i_rmin == MetricAxis.INDEX_TOO_SMALL ) {
					i_rmin = 0;
					obj_range[i][RMIN] = metric.low();
				}
				ranges[i][RMAX] = i_rmax;
				if ( i_rmax == MetricAxis.INDEX_TOO_LARGE ) {
					i_rmax = metric.axisSize();
					obj_range[i][RMAX] = metric.high();
				}
				ranges[i][RMIN] = i_rmin;
				ranges[i][RMAX] = i_rmax;
				if (doResultCorrection
						&& (!metric.exactIndex(obj_range[i][RMIN]) || !metric
								.exactIndex(obj_range[i][RMAX]))) {
					needsCorrection = true;
				}
			}
		}
		long direct_result = -1;
		long pg_direct_time_ms = -1;
		if ( useDirect ) {
			String daggr = "x"; 
			if ( aggr.equals("count") )
				daggr = "count(*)";
			else if ( aggr.equals("sumAggr") )
				daggr = "sum(sumAggr)";
			else
				throw new SQLException("unexpected aggr: "+aggr);
			String cond = "";
			for(i=0; i<axis.length; i++) {
				if ( i > 0 )
					cond += " AND ";
				cond += "(" + axis[i].columnExpression() + ">= ? ) AND (" + axis[i].columnExpression() + "< ?)";
			}
			pg_direct_time_ms = new Date().getTime();
			PreparedStatement ps = c.prepareStatement("SELECT "+daggr+" from "+schema+"."+table + " WHERE " + cond + ";");
			for(i=0; i<axis.length; i++) {
				ps.setObject(1 + i*2, obj_range[i][RMIN]);
				ps.setObject(2 + i*2, obj_range[i][RMAX]);
			}
			System.out.println("#! Direct query="+ps.toString());
			ResultSet rs = ps.executeQuery();
			pg_direct_time_ms = new Date().getTime() - pg_direct_time_ms;
			if ( !rs.next() )
				throw new RuntimeException("NO RESULT");
			direct_result = rs.getLong(1);
			System.out.println("# direct result = "+direct_result+", time = "+pg_direct_time_ms+"ms");
		}
		//
		PreparedStatement correction_stat = null;
		if ( needsCorrection && doResultCorrection )
			correction_stat = correctionStatement(ranges,obj_range);

		// kd.switchSubindexOff();

		String sqlaggr = "";
                String aggrCol = "";
                if (serversideStairwalk  && SqlUtils.dbType(c) == DbType.MONETDB) {
                    if ( aggr.equals("count") ) {
                        sqlaggr = "sum(aggr1) AS countAggr";
                        aggrCol = "countaggr";
                    } else if ( aggr.equals("sum") ) {
                        sqlaggr = "sum(aggr1) AS sumAggr";
                        aggrCol = "sumaggr";
                    } else if ( aggr.equals("min") ) {
                        sqlaggr = "min(aggr1) AS minAggr";
                        aggrCol = "minaggr";
                    } else if ( aggr.equals("max") ) {
                        sqlaggr = "max(aggr1) AS maxAggr";
                        aggrCol = "maxaggr";
                    } else {
                        throw new SQLException("unexpected aggr: "+aggr);
                    }
                } else {                
                    if ( aggr.equals("count") ) sqlaggr = "sum(countAggr)";
                    else if ( aggr.equals("sum") ) sqlaggr = "sum(sumAggr)";
                    else if ( aggr.equals("min") ) sqlaggr = "min(minAggr)";
                    else if ( aggr.equals("max") ) sqlaggr = "max(maxAggr)";
                    else throw new SQLException("unexpected aggr: "+aggr);
                }
                
		StringBuffer qb = new StringBuffer("SELECT ");
                qb.append(sqlaggr).append(" FROM ");
                
                // select directly from PA table when not doing serverside stairwalk
                // or when database is not MonetDB
                // because MonetDB does not need to select from PA table in serverside stairwalk
                if (serversideStairwalk == false || SqlUtils.dbType(c) != DbType.MONETDB) {                
                    qb.append(schema).append(".").append(table).append(PA_EXTENSION);
                }

		System.out.println("$ pa-command = "+range_paGridQuery(ranges));
		long internalCount = 0;
		if ( serversideStairwalk ) {
			// use internal pacells2d function
			String pa_grid_str;
                        
                        if (SqlUtils.dbType(c) == DbType.MONETDB) {
                            qb.append("pa_grid_enhanced(");
                            qb.append(SqlUtils.quoteValue(c, range_paGridQuery(ranges)));
                            qb.append(", ").append(SqlUtils.quoteValue(c, schema));
                            qb.append(", ").append(SqlUtils.quoteValue(c, table + PA_EXTENSION));
                            qb.append(", ").append(SqlUtils.quoteValue(c, "ckey"));
                            qb.append(", ").append(SqlUtils.quoteValue(c, aggrCol));
                            qb.append(")");
                        } else {
                            qb.append(", ");
                            
                            if (kd.kind() == AggrKeyDescriptor.KD_BYTE_STRING) {
                                // byte version doesn't have a cell function but also uses grid function
                                qb.append("pa_grid('").append(range_paGridQuery(ranges)).append("')");
                                qb.append(" WHERE ckey=pa_bytekey");
                            } else {                            
                                qb.append("pa_grid_cell('").append(range_paGridQuery(ranges)).append("') AS pakey ");
                                qb.append(" WHERE ckey=pakey");
                            }
                        }
		} else {
			Vector<AggrKey> resKeys;
			resKeys = computePaCells(kd,ranges,axis);
			
			System.out.println("#!create WHERE {ckey=v} query: #keys="+resKeys.size());
			qb.append(" WHERE ");
			for (i = 0; i < resKeys.size(); i++) {
				qb.append(((i > 0) ? " OR " : "") + "ckey="
						+ SqlUtils.quoteValue(c, resKeys.elementAt(i).toKey()));
			}
		}
		qb.append(";");
		long pg_time_ms = -1;
		long result = -1, correction = -1;
		if ( true ) {
			System.out.println("# main query= " + qb);
			pg_time_ms = new Date().getTime();
			result = SqlUtils.execute_1long(c, qb.toString());
			pg_time_ms = new Date().getTime() - pg_time_ms;
			System.out.println("# answer = "+result+", time = "+pg_time_ms+"ms");
			//
			if ( correction_stat != null ) {
				System.out.println("# corr query= " + correction_stat);
				pg_time_ms = new Date().getTime();
				ResultSet rs = correction_stat.executeQuery();
				if ( !rs.next() )
					throw new SQLException("correction result empty");
				correction = - rs.getLong(1)	;
				pg_time_ms = new Date().getTime() - pg_time_ms;
				System.out.println("# correction="+correction+", time = "+pg_time_ms+"ms");
			}
		}
		if (true) {
			System.out.println("# schema="+schema+", table="+table);
			System.out.print("# main answer="+result);
			if ( correction_stat != null )
				System.out.print(", corrected answer="+(result + correction));
			if ( useDirect )
				System.out.print(", direct answer="+direct_result+".");
			System.out.println("");
		}
		return result;
	}

	private PreparedStatement correctionStatement(int ranges[][], Object obj_range[][])
	throws SQLException {
		int i;
		Object rangesREV[][] = new Object[axis.length][2];

		for (i = 0; i < axis.length; i++) {
			rangesREV[i][RMIN] = axis[i].reverseValue(ranges[i][RMIN]);
			rangesREV[i][RMAX] = axis[i].reverseValue(ranges[i][RMAX] + 1);
		}
		StringBuilder sb = new StringBuilder();
		Vector<Object> all = new Vector<Object>();
		Object outer[][] = rangesREV;
		Object inner[][] = obj_range;

		for (i = 0; i < axis.length; i++) {
			StringBuilder lsb = new StringBuilder();
			Vector<Object> lv = new Vector<Object>();
			StringBuilder hsb = new StringBuilder();
			Vector<Object> hv = new Vector<Object>();

			String ci = axis[i].columnExpression();
			lsb.append(ci + ">=" + '?' + " AND " + ci + "<" + '?');
			lv.add(outer[i][RMIN]);
			lv.add(inner[i][RMIN]);
			hsb.append(ci + ">" + '?' + " AND " + ci + "<=" + '?');
			hv.add(inner[i][RMAX]);
			hv.add(outer[i][RMAX]);
			for (int j = 0; j < axis.length; j++)
				if (i != j) {
					String cj = axis[j].columnExpression();
					String di;
					Vector<Object> dv = new Vector<Object>();
					if (i < j) {
						di = cj + ">=" + '?' + " AND " + cj + "<=" + '?';
						dv.add(outer[j][RMIN]);
						dv.add(outer[j][RMAX]);
					} else {
						di = cj + ">" + '?' + " AND " + cj + "<" + '?';
						dv.add(inner[j][RMIN]);
						dv.add(inner[j][RMAX]);
					}
					lsb.append(" AND " + di);
					lv.addAll(dv);
					hsb.append(" AND " + di);
					hv.addAll(dv);
				}
			if (i > 0)
				sb.append(" OR ");
			sb.append("(" + lsb + ")\n");
			all.addAll(lv);
			sb.append(" OR (" + hsb + ")\n");
			all.addAll(hv);
		}
		PreparedStatement ps = c.prepareStatement("SELECT count(*) from " + schema + "." + table
				+ " WHERE " + sb + ";");
		for(i=0; i<all.size(); i++)
			ps.setObject(i+1,all.get(i));
		return ps;
	}

	private static boolean qverbose = false;

	public static final boolean sw_verbose = true;
	
	private static final Vector<Long> stairwalk(int from, int to, int N) {
		if ( sw_verbose )
			System.out.println("- stairwalk(from="+from+",to="+to+",N="+N+")");
		Vector<Long> res = new Vector<Long>();
		to++; // last step must be to 1 beyond upper bound
		short level = 0;
		int step  = 1;
		int nowAt = from;
		// first walk up the stairs
		while ( nowAt <= to ) {
			if (sw_verbose )
				System.out.println("* Check up: nowAt%(step*N)="+(nowAt % (step*N))+", (nowAt+step*N)="+(nowAt+step*N)+", level="+level+", step="+step+", to="+to);
			if ( (nowAt % (step*N) == 0) && ((nowAt+step*N)<=to) ) {
				// I can make a step up
				level++;
				step *= N;
				if ( sw_verbose )
					System.out.println("* Stepping down: nowAt="+nowAt+", level="+level+", step="+step);
			} else {
				// mark this one for selection
				if ( (nowAt+step) <= to ) {
					if ( sw_verbose )
						System.out.println("+ Adding: nowAt="+li_toString(li_key(level,nowAt/step)));
					res.add(li_key(level,nowAt/step));
				}
				nowAt += step;
			}
		}
		nowAt -= step;	// do step back you're too far
		if ( sw_verbose )
			System.out.println("* Stepping down: nowAt="+nowAt+", level="+level);
		while (nowAt < to) {
			// System.out.println("L["+level+"]: nowAt="+nowAt+", step="+step);
			if ((nowAt + step) > to) {
				// step down;
				level--;
				step /= N;
			} else {
				// make a step
				if ( (nowAt+step) <= to ) {
					res.add(li_key(level,nowAt/step));
					if ( sw_verbose )
						System.out.println("+ Adding: nowAt="+li_toString(li_key(level,nowAt/step)));
					nowAt += step;
				}
			}
		}
		if ( true ) {
			System.out.println("# do stairwalk(from="+from+",to="+(to-1)+",N="+N+") = {");
			for(int i=0; i<res.size(); i++)
				System.out.println("\t"+li_toString(res.elementAt(i).longValue()));
			System.out.println("}");
		}
		return res;
	}

	protected String empty_paGridQuery() { // just for key manipulations
		int swgc[][] = new int[axis.length][3]; // start/width/gridcells

		for(int i=0; i<axis.length; i++) {
			swgc[i][0] = 0; // start
			swgc[i][1] = 0; // width
			swgc[i][2] = 0; // gridcells
		}
		return grid_paGridQuery(swgc);
	}

	protected String range_paGridQuery(int range[][]) {
		int swgc[][] = new int[range.length][3]; // start/width/gridcells

		for(int i=0; i<range.length; i++) {
			swgc[i][0] = range[i][RMIN]; // start
			if ( (range[i][RMAX]<0) || (range[i][RMIN]<0) )
				swgc[i][1] = 0;
			else
				swgc[i][1] = range[i][RMAX] - range[i][RMIN]; // width
			swgc[i][2] = 1; // gridcells
		}
		return grid_paGridQuery(swgc);
	}

	protected String grid_paGridQuery(int swgc[][]) {
		if ( (swgc.length != axis.length) || (swgc[0].length != 3) )
			throw new RuntimeException("Dimensions for grid_paQuery wrong");
		StringBuffer sb = new StringBuffer();
                
                // start grid query
                sb.append("#G|");
                
                // add type of AggrKey
                sb.append(kd.kind()).append("|");
                
                // add size of level
                sb.append(kd.levelBits).append("|");
                
                // number of dimensions		
		sb.append(kd.dimensions()+"|");
                
                // add info of each dimension
		for(int i=0; i<swgc.length; i++) {
                    sb.append(axis[i].N()).append(",");                 
                    sb.append(kd.dimBits[i]).append(",");
                                        
                    sb.append(swgc[i][0]+","+swgc[i][1]+","+swgc[i][2]+"|");
		}
		sb.append(schema+"."+table+"|"+schema+"."+table+"_btree"+"|");
		if ( false ) System.out.println("XX="+sb);
		return sb.toString();
	}

	private static Vector<AggrKey> computePaCells(AggrKeyDescriptor kd, int range[][], AggregateAxis axis[]) {
		Vector<Vector<Long>> stairs = new Vector<Vector<Long>>();

		PermutationGenerator p = new PermutationGenerator(axis.length);
		for(int i=0; i<axis.length; i++) {
			Vector<Long> sw_res = stairwalk(range[i][RMIN], range[i][RMAX], axis[i].N());
			stairs.add(sw_res);
			p.setRange(i,0,sw_res.size());
		}
		AggrKey K = new AggrKey(kd);
		Vector<AggrKey> res = new Vector<AggrKey>();
		p.start();
		while ( p.next() ) {
			K.reset();
			for(short i=0; i<axis.length; i++) {
				long lk = stairs.elementAt(i).elementAt(p.permutation(i));
				K.setIndex(i, li_i(lk));
				K.setLevel(i, li_l(lk));
			}
			res.add( K.copy() );
		}
		if ( sw_verbose ) {
			System.out.println("- RESULT(" + res.size() + ") = {");
			for (int i = 0; i < res.size(); i++)
				System.out.println("\t" + res.elementAt(i));
			System.out.println("}");
		}
		return res;
	}

	/*
	 * The repository management section.
	 */
	private static void checkRepository(Connection c, String schema) throws SQLException{
		// create one repository per schema
		if (!SqlUtils.existsTable(c, schema, aggregateRepositoryName)) {
                        LOGGER.info("Main PA index repository does not exist yet, creating now...");
			SqlUtils.executeNORES(c,
					"CREATE TABLE " + schema + "." + aggregateRepositoryName + " (" +
					"tableName TEXT," +
					"label TEXT," +
					"aggregateColumn TEXT," +
					"aggregateType TEXT," +
					"dimensions int," +
					"keyflag char," +
					"aggregateMask int," +
					"count int" +
					");"
			);
                        LOGGER.info("Main PA index repository created");
		}

		if (!SqlUtils.existsTable(c, schema, aggregateRepositoryDimName)) {
                        LOGGER.info("Secondary PA axis index repository does not exist yet, creating now...");
			SqlUtils.executeNORES(c, 
					"CREATE TABLE " + schema + "." + aggregateRepositoryDimName + " (" +
					"tableName TEXT," + 
					"label TEXT," +
					"dimension int," +
					"columnExpression TEXT," + 
					"type TEXT," +
					"low TEXT," + 
					"high TEXT," + 
					"BASEBLOCKSIZE TEXT," +
					"N int," +
					"Nmax int," +
					"bits int" +
			");");
                        LOGGER.info("Secondary PA axis index created");
		}
	}

	private boolean read_repository(Connection c, String schema, String tableName, String label) throws SQLException {
		if(!SqlUtils.existsTable(c, schema, tableName)) return false; // CHECK Andreas Change
		ResultSet rs = SqlUtils.execute(c,
				"SELECT " + "dimensions,keyflag,aggregatemask,aggregatecolumn,aggregatetype" +
				" FROM " + schema + "." + aggregateRepositoryName +
				" WHERE tableName=\'"+tableName+"\' AND label=\'"+label+"\';"
		);

		if (!rs.next())
			return false;
		int dimensions = rs.getInt(1);
		char keyFlag = rs.getString(2).charAt(0);
		aggregateMask = rs.getInt(3);
		aggregateColumn = rs.getString(4);
		aggregateType = rs.getString(5);
		AggregateAxis read_axis[] = new AggregateAxis[dimensions];

		ResultSet rsi = SqlUtils.execute(c,
				"SELECT "+"dimension,columnExpression,type,low,high,BASEBLOCKSIZE,N"+" FROM " + schema + "." + aggregateRepositoryDimName +
				" WHERE tableName=\'"+tableName+"\' AND label=\'"+label+"\';"
		);
		while( rsi.next() ) {
			int rsi_i = rsi.getInt(1);
			String rsi_column = rsi.getString(2);
			String rsi_type = rsi.getString(3);
			String rsi_low = rsi.getString(4);
			String rsi_high = rsi.getString(5);
			String rsi_BBS = rsi.getString(6);
			short rsi_N = (short)rsi.getInt(7);
				
			if (rsi_type.equals("nominal"))
				if ( rsi_low.equals(NominalAxis.WORDLISTNOMINAL) )
					read_axis[rsi_i] = new NominalAxis(null,rsi_column,rsi_high);
				else
					read_axis[rsi_i] = new NominalAxis(rsi_column,Integer.parseInt(rsi_low), Integer.parseInt(rsi_high));
			else
				read_axis[rsi_i] = new MetricAxis(rsi_column, rsi_type,
						rsi_low, rsi_high, rsi_BBS, rsi_N);
		}
		for (int i=0; i<dimensions; i++) {
			if ( read_axis[i] == null )
				throw new NullPointerException();
		}
		axis = read_axis;
                
                try {
                    // INCOMPLETE, put in flag type
                    kd = new AggrKeyDescriptor(keyFlag,axis); // INCOMPLETE, put in flag type
                } catch (AggrKeyDescriptor.TooManyBitsException ex) {
                    LOGGER.warn("Too many bits > 63");
                }
		//
		return true;
	}

	protected static void update_repository(Connection c, String schema, String tableName, String label, String aggregateColumn, String aggregateType, AggrKeyDescriptor kd, AggregateAxis axis[], int aggregateMask)
	throws SQLException {
                LOGGER.info("Adding PreAggregate index to PA index repository...");
                
		checkRepository(c,schema);
                
		SqlUtils.executeNORES(c,
				"DELETE FROM " + schema + "." + aggregateRepositoryName +
				" WHERE tableName=\'"+tableName+"\' AND label=\'"+label+"\';"
		);
		SqlUtils.executeNORES(c,
				"DELETE FROM " + schema + "." + aggregateRepositoryDimName +
				" WHERE tableName=\'"+tableName+"\' AND label=\'"+label+"\';"
		);
		long cnt = SqlUtils.count(c, schema, tableName, "*");
		PreparedStatement ps = c.prepareStatement("INSERT INTO " + schema + "." + aggregateRepositoryName + "  (" +
				"tableName," +
				"label," +
				"aggregateColumn,"+
				"aggregateType,"+
				"dimensions," +
				"keyflag," +
				"aggregateMask," +
				"count"+
		") VALUES(?,?,?,?,?,?,?,?);");
		PreparedStatement psi = c.prepareStatement("INSERT INTO " + schema + "." + aggregateRepositoryDimName + "  (" +
				"tableName," +
				"label," +
				"dimension," +
				"columnExpression," + 
				"type," +
				"low," + 
				"high," + 
				"BASEBLOCKSIZE," +
				"N," +
				"Nmax," +
				"bits"  +
		") VALUES(?,?,?,?,?,?,?,?,?,?,?);");
		ps.setString(1,tableName);
		ps.setString(2,label);
		ps.setString(3,aggregateColumn);
		ps.setString(4,aggregateType);
		ps.setInt(5,axis.length);
		ps.setString(6,String.valueOf(kd.kind())); // INCOMPLETE
		ps.setInt(7,aggregateMask);
		ps.setInt(8, (int)cnt);
		for (int i = 0; i < axis.length; i++) {
			psi.setString(1, tableName);
			psi.setString(2, label);
			psi.setInt(3, i);
			psi.setString(4, axis[i].columnExpression());
			psi.setString(5, axis[i].type());
			if (axis[i].isMetric()) {
				MetricAxis metric = (MetricAxis) axis[i];
				psi.setString(6, metric.storageFormat(metric.low()));
				psi.setString(7, metric.storageFormat(metric.high()));
				psi.setString(8, metric.BASEBLOCKSIZE().toString());
			} else {
				NominalAxis nominal = (NominalAxis) axis[i];
				psi.setString(6, nominal.storageFormat(nominal.fromFIELDstore()));
				psi.setString(7, nominal.storageFormat(nominal.toFIELDstore()));
				psi.setString(8, "");
			}
			psi.setInt(9, axis[i].N());
			psi.setInt(10, axis[i].maxLevels());
			psi.setInt(11, axis[i].bits());
			psi.execute();
		}
		ps.execute();
                
                LOGGER.info("Index has been added to index repository");
	}

	public AggregateAxis[] getAxis(){
		return axis;
	}

	public int getAggregateMask() {
		return aggregateMask;
	}

	public String getSchema() {
		return schema;
	}

	public String getTable() {
		return table;
	}

	public String getLabel() {
		return label;
	}

}
