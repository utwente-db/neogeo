package nl.utwente.db.neogeo.preaggregate.mysql;

public class MysqlOrderQueryBuilder {
	public static final int MEDIAN = 1;
	public static final int GENERIC = 2;
	public static final int PERCENTILE_10 = 4;
	public static final int PERCENTILE_20 = 8;
	public static final int PERCENTILE_30 = 16;
	public static final int PERCENTILE_40 = 32;
	public static final int PERCENTILE_50 = 64;
	public static final int PERCENTILE_60 = 128;
	public static final int PERCENTILE_70 = 256;
	public static final int PERCENTILE_80 = 1024;
	public static final int PERCENTILE_90 = 2048;

	public static String getLongBlobToIntFunctionDefinition(){
		StringBuffer sb = new StringBuffer();
		sb.append("DELIMITER //\n");
		sb.append("DROP FUNCTION IF EXISTS _ipfx_longblobToInt //\n");
		sb.append("CREATE DEFINER=`%`@`%` FUNCTION `_ipfx_longblobToInt`(`_bin` binary(2))\n");
		sb.append("RETURNS int(11)\n");
		sb.append("LANGUAGE SQL\n");
		sb.append("DETERMINISTIC\n");
		sb.append("CONTAINS SQL\n");
		sb.append("SQL SECURITY DEFINER\n");
		sb.append("COMMENT ''\n");
		sb.append("BEGIN\n");
		sb.append("DECLARE offset int;\n");
		sb.append("   SET offset = power(-1,((ord(substring(_bin,1, 1)) & 0x80)=0)+1);\n");
		sb.append("   return offset *( (ord(substring(_bin,1,1))&0x7f)*0x1000000+\n");
		sb.append("                    (ord(substring(_bin,2,1))&0xff)*0x10000+\n");
		sb.append("                    (ord(substring(_bin,3,1))&0xff)*0x100+\n");
		sb.append("                    (ord(substring(_bin,4,1))&0xff));\n");
		sb.append("END//\n");
		sb.append("DELIMITER ;\n");
		return sb.toString();
	}

	public static String getPAOrderTable(int aggregates){
		StringBuffer sb = new StringBuffer();
		sb.append("DROP TABLE IF EXISTS pegel_andelfingen2_pa_order;\n");
		sb.append("CREATE TABLE pegel_andelfingen2_pa_order (\n");
		//sb.append("    `l0` INT(10) NOT NULL,\n");
		//sb.append("    `i0` INT(11) NOT NULL,\n");
		sb.append("    `base_id` BIGINT(21) NOT NULL,\n");
		sb.append("    `cnt` INT(11) NOT NULL DEFAULT '0',\n");
		if((aggregates & MysqlOrderQueryBuilder.MEDIAN) == MysqlOrderQueryBuilder.MEDIAN){
			sb.append("    `median_start` LONGBLOB NULL,\n");
			sb.append("    `median_end` LONGBLOB NULL,\n");
		}
		if((aggregates & MysqlOrderQueryBuilder.PERCENTILE_10) == MysqlOrderQueryBuilder.PERCENTILE_10){
			sb.append("    `p10_start` LONGBLOB NULL,\n");
			sb.append("    `p10_end` LONGBLOB NULL,\n");
		}
		if((aggregates & MysqlOrderQueryBuilder.PERCENTILE_20) == MysqlOrderQueryBuilder.PERCENTILE_20){
			sb.append("    `p20_start` LONGBLOB NULL,\n");
			sb.append("    `p20_end` LONGBLOB NULL,\n");
		}
		if((aggregates & MysqlOrderQueryBuilder.PERCENTILE_30) == MysqlOrderQueryBuilder.PERCENTILE_30){
			sb.append("    `p30_start` LONGBLOB NULL,\n");
			sb.append("    `p30_end` LONGBLOB NULL,\n");
		}
		if((aggregates & MysqlOrderQueryBuilder.PERCENTILE_40) == MysqlOrderQueryBuilder.PERCENTILE_40){
			sb.append("    `p40_start` LONGBLOB NULL,\n");
			sb.append("    `p40_end` LONGBLOB NULL,\n");
		}
		if((aggregates & MysqlOrderQueryBuilder.PERCENTILE_50) == MysqlOrderQueryBuilder.PERCENTILE_50){
			sb.append("    `p50_start` LONGBLOB NULL,\n");
			sb.append("    `p50_end` LONGBLOB NULL,\n");
		}
		if((aggregates & MysqlOrderQueryBuilder.PERCENTILE_60) == MysqlOrderQueryBuilder.PERCENTILE_60){
			sb.append("    `p60_start` LONGBLOB NULL,\n");
			sb.append("    `p60_end` LONGBLOB NULL,\n");
		}
		if((aggregates & MysqlOrderQueryBuilder.PERCENTILE_70) == MysqlOrderQueryBuilder.PERCENTILE_70){
			sb.append("    `p70_start` LONGBLOB NULL,\n");
			sb.append("    `p70_end` LONGBLOB NULL,\n");
		}
		if((aggregates & MysqlOrderQueryBuilder.PERCENTILE_80) == MysqlOrderQueryBuilder.PERCENTILE_80){
			sb.append("    `p80_start` LONGBLOB NULL,\n");
			sb.append("    `p80_end` LONGBLOB NULL,\n");
		}
		if((aggregates & MysqlOrderQueryBuilder.PERCENTILE_90) == MysqlOrderQueryBuilder.PERCENTILE_90){
			sb.append("    `p90_start` LONGBLOB NULL,\n");
			sb.append("    `p90_end` LONGBLOB NULL,\n");
		}
		sb.append("    `order_map` LONGBLOB NULL);\n");
		return sb.toString();
	}

	public static String fillPreaggregateTable(long cnt){
		StringBuffer sb = new StringBuffer();
		sb.append("SET SESSION group_concat_max_len = 4*"+cnt+";\n");
		sb.append("SET SESSION max_allowed_packet =4*"+cnt+";\n"); 
		sb.append("delete from datagraph.pegel_andelfingen2_pa_order;\n");
		sb.append("insert into datagraph.pegel_andelfingen2_pa_order (base_id,cnt,order_map)\n");
		sb.append("Select min(id) as base_id, count(*) as cnt, \n");
		sb.append("    group_concat(concat(\n");
		sb.append("              char(rank_new div 256*256*256),\n");
		sb.append("              char(rank_new div 256*256),\n");
		sb.append("              char(rank_new div 256),\n");
		sb.append("              char(rank_new mod 256)) order by id separator '')\n");
		sb.append("from \n");
		sb.append("   (select id,\n");
		sb.append("      ( @vcurRow := @vcurRow + 1) AS rank_new, pegel\n");
		sb.append("	   from \n");
		sb.append("	     (select id, pegel\n"); 
		//sb.append("		    ,(((_ipfx_d0rf(timed)-dim00.offset*power(4,dim0.`level`-1)) div dim0.factor)*4+dim00.offset) as v0\n");
		sb.append("		  from pegel_andelfingen2) B, \n");
		sb.append("       (SELECT @vcurRow := 0) r\n");
		sb.append("    order by B.PEGEL asc ) O;\n");
		return sb.toString();
	}
	
}	
