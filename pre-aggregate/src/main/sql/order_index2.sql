# use this information for dimensionig the tabples below
select _ipfx_revKeyLevel(`key`) as l0, _ipfx_revKeyIndex(`key`) as v0, max(cval) 
from _ipfx_delta 
group by _ipfx_revKeyLevel(`key`);
# result:
# l0,v0,max(cval)
# 0,0,3   -> 1Byte
# 1,0,11   -> 1Byte
# 2,0,29   -> 1Byte
# 3,0,78   -> 1Byte
# 4,0,286   -> 2Bytes
# 5,0,1065   -> 2Bytes
# 6,0,4146   -> 2Bytes
# 7,0,16464   -> 2Bytes
# 8,0,65756   -> 3Bytes
# 9,0,262344   -> 3Bytes
# 10,0,1048697   -> 3Bytes
# 11,0,2376930   -> 3Bytes

#Depending on the length, you'll get:
#       0 < length <=      255  -->  `TINYBLOB`
#     255 < length <=    65535  -->  `BLOB`
#   65535 < length <= 16777215  -->  `MEDIUMBLOB`
#16777215 < length <=    2³¹-1  -->  `LONGBLOB`

SET SESSION group_concat_max_len = 5376930*3;
SET GLOBAL max_allowed_packet = 5376930*3;


DROP TABLE IF EXISTS pegel_andelfingen2_pa_order_tinyblob;
CREATE TABLE pegel_andelfingen2_pa_order_tinyblob (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cnt` INT(11) NOT NULL DEFAULT '0',
	`order_map` BLOB NOT NULL,
	`median_start` BLOB NULL,
	`median_end` BLOB NULL,
	PRIMARY KEY (`l0`, `i0`)
);

DROP TABLE IF EXISTS pegel_andelfingen2_pa_order_blob;
CREATE TABLE pegel_andelfingen2_pa_order_blob (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cnt` INT(11) NOT NULL DEFAULT '0',
	`order_map` BLOB NOT NULL,
	`median_start` BLOB NULL,
	`median_end` BLOB NULL,
	PRIMARY KEY (`l0`, `i0`)
);

DROP TABLE IF EXISTS pegel_andelfingen2_pa_order_mediumblob;
CREATE TABLE pegel_andelfingen2_pa_order_mediumblob (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cnt` INT(11) NOT NULL DEFAULT '0',
	`order_map` MEDIUMBLOB NOT NULL,
	`median_start` MEDIUMBLOB NULL,
	`median_end` MEDIUMBLOB NULL,
	PRIMARY KEY (`l0`, `i0`)
);

DROP TABLE IF EXISTS pegel_andelfingen2_pa_order_longblob;
CREATE TABLE pegel_andelfingen2_pa_order_longblob (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cnt` INT(11) NOT NULL DEFAULT '0',
	`order_map` LONGBLOB NOT NULL,
	`median_start` LONGBLOB NULL,
	`median_end` LONGBLOB NULL,
	PRIMARY KEY (`l0`, `i0`)
);

DELIMITER //
drop function if exists `_ipfx_tinyblobToInt`//
CREATE DEFINER=`root`@`%` FUNCTION `_ipfx_tinyblobToInt`(`_bin` binary(1))
	RETURNS int(11)
	LANGUAGE SQL
	DETERMINISTIC
	CONTAINS SQL
	SQL SECURITY DEFINER
	COMMENT ''
BEGIN
	DECLARE offset int;
	SET offset = power(-1,((ord(substring(_bin,1, 1)) & 0x80)=0)+1);
	return offset *( (ord(substring(_bin,1,1))&0x7f));
END//

drop function if exists `_ipfx_blobToInt`//
CREATE DEFINER=`root`@`%` FUNCTION `_ipfx_blobToInt`(`_bin` binary(2))
	RETURNS int(11)
	LANGUAGE SQL
	DETERMINISTIC
	CONTAINS SQL
	SQL SECURITY DEFINER
	COMMENT ''
BEGIN
	DECLARE offset int;
	SET offset = power(-1,((ord(substring(_bin,1, 1)) & 0x80)=0)+1);
	return offset *( (ord(substring(_bin,1,1))&0x7f)*0x100+
		 		 		  (ord(substring(_bin,2,1))&0xff));
END//

drop function if exists `_ipfx_mediumblobToInt`//
CREATE DEFINER=`root`@`%` FUNCTION `_ipfx_mediumblobToInt`(`_bin` binary(3))
	RETURNS int(11)
	LANGUAGE SQL
	DETERMINISTIC
	CONTAINS SQL
	SQL SECURITY DEFINER
	COMMENT ''
BEGIN
	DECLARE offset int;
	SET offset = power(-1,((ord(substring(_bin,1, 1)) & 0x80)=0)+1);
	return offset *( (ord(substring(_bin,1,1))&0x7f)*0x10000+
						  (ord(substring(_bin,2,1))&0xff)*0x100+
		 		 		  (ord(substring(_bin,3,1))&0xff));
END//

drop function if exists `_ipfx_longblobToInt`//
CREATE DEFINER=`root`@`%` FUNCTION `_ipfx_longblobToInt`(`_bin` binary(4))
	RETURNS int(11)
	LANGUAGE SQL
	DETERMINISTIC
	CONTAINS SQL
	SQL SECURITY DEFINER
	COMMENT ''
BEGIN
	DECLARE offset int;
	SET offset = power(-1,((ord(substring(_bin,1, 1)) & 0x80)=0)+1);
	return offset *( (ord(substring(_bin,1,1))&0x7f)*0x1000000+
						  (ord(substring(_bin,2,1))&0xff)*0x10000+
		 		 		  (ord(substring(_bin,3,1))&0xff)*0x100+
						  (ord(substring(_bin,4,1))&0xff));
END//
DELIMITER ;

# create level 4 - 7
# everything which handeled with 2 bytes
delete from datagraph.pegel_andelfingen2_pa_order_blob;
insert into datagraph.pegel_andelfingen2_pa_order_blob (l0,i0,base_id,cnt,order_map)
Select l0, v0 , min(id) as base_id, count(*) as cnt, group_concat(concat(char(rank_new div 256),char(rank_new mod 256)) order by id separator '')
from 
	(select l0, v0, id,
           ( CASE (v0)
                 WHEN @vbucket 
                 THEN @vcurRow := @vcurRow + 1 
                 ELSE @vcurRow := 1 AND @vbucket := v0 
    			END
           )  AS rank_new, @vbucket, pegel
	 from 
	 	(select dim0.`level` as l0,  id, pegel, 
		 			(((_ipfx_d0rf(timed)-dim00.offset*power(4,dim0.`level`-1)) div dim0.factor)*4+dim00.offset) as v0
		 from pegel_andelfingen2, _ipfx_dim0 AS dim0, _ipfx_dim0_offset dim00
		 where dim0.`level` >= 4 and dim0.`level` <= 7  ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

# create level 8 - 11
# everything which handeled with 3 bytes
delete from datagraph.pegel_andelfingen2_pa_order_mediumblob;
insert into datagraph.pegel_andelfingen2_pa_order_mediumblob (l0,i0,base_id,cnt,order_map)
Select l0, v0 , min(id) as base_id, count(*) as cnt, group_concat(concat(char(rank_new div 65536), char(rank_new div 256),char(rank_new mod 256)) order by id separator '')
from 
	(select l0, v0, id,
           ( CASE (v0)
                 WHEN @vbucket 
                 THEN @vcurRow := @vcurRow + 1 
                 ELSE @vcurRow := 1 AND @vbucket := v0 
    			END
           )  AS rank_new, @vbucket, pegel
	 from 
	 	(select dim0.`level` as l0,  id, pegel, 
		 			(((_ipfx_d0rf(timed)-dim00.offset*power(4,dim0.`level`-1)) div dim0.factor)*4+dim00.offset) as v0
		 from pegel_andelfingen2, _ipfx_dim0 AS dim0, _ipfx_dim0_offset dim00
		 where dim0.`level` >= 8 and dim0.`level` <= 11 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

## old ##################


select *,  substring(order_map,3,cval-3),substring(order_map,-1), substring(order_map,1,3-1)
from pegel_andelfingen_pa_order
where level=1 and `index`=1;

# experiment
delete from datagraph._ipfx_level0_order_2;
insert into datagraph._ipfx_level0_order_2
Select l0, v0 , count(*) as cval, group_concat(char(rank_new) order by id separator '')
from 
	(select l0, v0, id,
           ( CASE (v0)
                 WHEN @vbucket 
                 THEN @vcurRow := @vcurRow + 1 
                 ELSE @vcurRow := 1 AND @vbucket := v0 
    			END
           )  AS rank_new, @vbucket, pegel
	 from 
	 	(select dim0.`level` as l0, ((_ipfx_d0rf(timed) - off0.`offset`) div dim0.factor)*4+ off0.`offset` as v0, id, pegel 
		 from pegel_andelfingen2, _ipfx_dim0 AS dim0, _ipfx_dim0_offset off0
		 where dim0.`level` = 2 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

select (ord(substring(median_start,1, 1)) & 0x80)>0 as sig_bit,
		 (ord(substring(median_start,1,1))&0x7f)*0x10000+(ord(substring(median_start,2,1))&0xff)*0x100+
		 		 (ord(substring(median_start,3,1))&0xff) as cnt
		 from _ipfx_level0_order_8 
where i0=0;
