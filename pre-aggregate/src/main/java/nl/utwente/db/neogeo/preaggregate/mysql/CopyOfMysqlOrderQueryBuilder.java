package nl.utwente.db.neogeo.preaggregate.mysql;

public class CopyOfMysqlOrderQueryBuilder {
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


	public String getBlobToIntFunctionDefinition(){
		StringBuffer sb = new StringBuffer();
		sb.append("DELIMITER //\n");
		sb.append("DROP FUNCTION IF EXISTS _ipfx_blobToInt //\n");
		sb.append("CREATE DEFINER=`root`@`%` FUNCTION `_ipfx_blobToInt`(`_bin` binary(2))\n");
		sb.append("RETURNS int(11)\n");
		sb.append("LANGUAGE SQL\n");
		sb.append("DETERMINISTIC\n");
		sb.append("CONTAINS SQL\n");
		sb.append("SQL SECURITY DEFINER\n");
		sb.append("COMMENT ''\n");
		sb.append("BEGIN\n");
		sb.append("DECLARE offset int;\n");
		sb.append("   SET offset = power(-1,((ord(substring(_bin,1, 1)) & 0x80)=0)+1);\n");
		sb.append("   return offset *( (ord(substring(_bin,1,1))&0x7f)*0x100+\n");
		sb.append("                    (ord(substring(_bin,2,1))&0xff));\n");
		sb.append("END//\n");
		sb.append("DELIMITER ;\n");
		return sb.toString();
	}

	public String getMediumBlobToIntFunctionDefinition(){
		StringBuffer sb = new StringBuffer();
		sb.append("DELIMITER //\n");
		sb.append("DROP FUNCTION IF EXISTS _ipfx_mediumblobToInt //\n");
		sb.append("CREATE DEFINER=`root`@`%` FUNCTION `_ipfx_mediumblobToInt`(`_bin` binary(2))\n");
		sb.append("RETURNS int(11)\n");
		sb.append("LANGUAGE SQL\n");
		sb.append("DETERMINISTIC\n");
		sb.append("CONTAINS SQL\n");
		sb.append("SQL SECURITY DEFINER\n");
		sb.append("COMMENT ''\n");
		sb.append("BEGIN\n");
		sb.append("DECLARE offset int;\n");
		sb.append("   SET offset = power(-1,((ord(substring(_bin,1, 1)) & 0x80)=0)+1);\n");
		sb.append("   return offset *( (ord(substring(_bin,1,1))&0x7f)*0x10000+\n");
		sb.append("              	   (ord(substring(_bin,2,1))&0xff)*0x100+\n");
		sb.append("                    (ord(substring(_bin,3,1))&0xff));\n");
		sb.append("END//\n");
		sb.append("DELIMITER ;\n");
		return sb.toString();
	}

	public String getLongBlobToIntFunctionDefinition(){
		StringBuffer sb = new StringBuffer();
		sb.append("DELIMITER //\n");
		sb.append("DROP FUNCTION IF EXISTS _ipfx_longblobToInt //\n");
		sb.append("CREATE DEFINER=`root`@`%` FUNCTION `_ipfx_longblobToInt`(`_bin` binary(2))\n");
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

	public String getPAOrderTable(String blobext, int aggregates){
		StringBuffer sb = new StringBuffer();
		sb.append("DROP TABLE IF EXISTS pegel_andelfingen2_pa_order_"+blobext+"blob;\n");
		sb.append("CREATE TABLE pegel_andelfingen2_pa_order_"+blobext+"blob (\n");
		sb.append("    `l0` INT(10) NOT NULL,\n");
		sb.append("    `i0` INT(11) NOT NULL,\n");
		sb.append("    `base_id` BIGINT(21) NOT NULL,\n");
		sb.append("    `cnt` INT(11) NOT NULL DEFAULT '0',\n");
		sb.append("    `order_map` BLOB NULL,\n");
		if((aggregates & CopyOfMysqlOrderQueryBuilder.MEDIAN) == CopyOfMysqlOrderQueryBuilder.MEDIAN){
			sb.append("    `median_start` BLOB NULL,\n");
			sb.append("    `median_end` BLOB NULL,\n");
		}
		if((aggregates & CopyOfMysqlOrderQueryBuilder.PERCENTILE_10) == CopyOfMysqlOrderQueryBuilder.PERCENTILE_10){
			sb.append("    `p10_start` BLOB NULL,\n");
			sb.append("    `p10_end` BLOB NULL,\n");
		}
		if((aggregates & CopyOfMysqlOrderQueryBuilder.PERCENTILE_20) == CopyOfMysqlOrderQueryBuilder.PERCENTILE_20){
			sb.append("    `p20_start` BLOB NULL,\n");
			sb.append("    `p20_end` BLOB NULL,\n");
		}
		if((aggregates & CopyOfMysqlOrderQueryBuilder.PERCENTILE_30) == CopyOfMysqlOrderQueryBuilder.PERCENTILE_30){
			sb.append("    `p30_start` BLOB NULL,\n");
			sb.append("    `p30_end` BLOB NULL,\n");
		}
		if((aggregates & CopyOfMysqlOrderQueryBuilder.PERCENTILE_40) == CopyOfMysqlOrderQueryBuilder.PERCENTILE_40){
			sb.append("    `p40_start` BLOB NULL,\n");
			sb.append("    `p40_end` BLOB NULL,\n");
		}
		if((aggregates & CopyOfMysqlOrderQueryBuilder.PERCENTILE_50) == CopyOfMysqlOrderQueryBuilder.PERCENTILE_50){
			sb.append("    `p50_start` BLOB NULL,\n");
			sb.append("    `p50_end` BLOB NULL,\n");
		}
		if((aggregates & CopyOfMysqlOrderQueryBuilder.PERCENTILE_60) == CopyOfMysqlOrderQueryBuilder.PERCENTILE_60){
			sb.append("    `p60_start` BLOB NULL,\n");
			sb.append("    `p60_end` BLOB NULL,\n");
		}
		if((aggregates & CopyOfMysqlOrderQueryBuilder.PERCENTILE_70) == CopyOfMysqlOrderQueryBuilder.PERCENTILE_70){
			sb.append("    `p70_start` BLOB NULL,\n");
			sb.append("    `p70_end` BLOB NULL,\n");
		}
		if((aggregates & CopyOfMysqlOrderQueryBuilder.PERCENTILE_80) == CopyOfMysqlOrderQueryBuilder.PERCENTILE_80){
			sb.append("    `p80_start` BLOB NULL,\n");
			sb.append("    `p80_end` BLOB NULL,\n");
		}
		if((aggregates & CopyOfMysqlOrderQueryBuilder.PERCENTILE_90) == CopyOfMysqlOrderQueryBuilder.PERCENTILE_90){
			sb.append("    `p90_start` BLOB NULL,\n");
			sb.append("    `p90_end` BLOB NULL,\n");
		}
		sb.append("    PRIMARY KEY (`l0`, `i0`);\n");
		return sb.toString();
	}

	public String getPreaggregateTable(String blobext){
		StringBuffer sb = new StringBuffer();
		int min=0;
		int max=0;
		int bytes=1;
		for(int i=0;i<MysqlConnectionOrder.coding.length;i++){
			if(blobext.isEmpty()){
				// 2 bytes
				bytes=2;
				if(min==0) min=i;
			} else if(blobext.equals("medium")){
				// 3 bytes
				bytes=3;
				if(min==0) min=i;
			} else if(blobext.equals("long")){
				// 4 bytes
				bytes=4;
				if(min==0) min=i;				
			} else if(min>0){
				max=i-1;
				break;
			}
		}
			
		sb.append("delete from datagraph.pegel_andelfingen2_pa_order_blob;\n");
		sb.append("insert into datagraph.pegel_andelfingen2_pa_order_blob (l0,i0,base_id,cnt,order_map)\n");
		sb.append("Select l0, v0 , min(id) as base_id, count(*) as cnt, \n");
		sb.append("group_concat(concat(");
		if(bytes>=4)
			sb.append("char(rank_new div 256*256*256),");
		if(bytes>=3)
			sb.append("char(rank_new div 256*256),");
		if(bytes>=2)
			sb.append("char(rank_new div 256),");
		if(bytes>=1)
			sb.append("char(rank_new mod 256)) order by id separator '')\n");
		sb.append("from \n");
		sb.append("   (select l0, v0, id,\n");
		sb.append("      ( CASE (v0)\n");
		sb.append("          WHEN @vbucket\n"); 
		sb.append("          THEN @vcurRow := @vcurRow + 1\n"); 
		sb.append("          ELSE @vcurRow := 1 AND @vbucket := v0\n"); 
		sb.append(" 	   END\n");
		sb.append("      )  AS rank_new, @vbucket, pegel\n");
		sb.append("	   from \n");
		sb.append("	     (select dim0.`level` as l0,  id, pegel,\n"); 
		sb.append("		    (((_ipfx_d0rf(timed)-dim00.offset*power(4,dim0.`level`-1)) div dim0.factor)*4+dim00.offset) as v0\n");
		sb.append("		  from pegel_andelfingen2, _ipfx_dim0 AS dim0, _ipfx_dim0_offset dim00\n");
		sb.append("		  where dim0.`level` >= "+min+" and dim0.`level` <= "+max+"  ) B, \n");
		sb.append("       (SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r\n");
		sb.append("    order by v0 asc, B.PEGEL asc ) O\n");
		sb.append("group by l0,v0;\n");
		return sb.toString();
	}
	
	public String getDimOffest(int N){
		StringBuffer sb = new StringBuffer();
		sb.append("CREATE TABLE `_ipfx_dim0_offset`\n"); 
		sb.append("		`offset` INT(10) NULL DEFAULT NULL;\n\n");
		for(int i=0;i<N;i++)
			sb.append("insert into _ipfx_dim0_offset set offset="+i+";\n");
		return sb.toString();
	}
	
	
}	
