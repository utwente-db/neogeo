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

SET SESSION group_concat_max_len = 2376930*3;

DROP TABLE IF EXISTS `pegel_andelfingen2_pa_order`;
CREATE TABLE `pegel_andelfingen2_pa_order` (
	`key` BIGINT(20) NOT NULL,
	`level` INT NOT NULL,
	`index` INT NOT NULL,
	`cval` BIGINT(20) NOT NULL,
	`order_map` BINARY(15) NOT NULL,
	PRIMARY KEY (`key`)
);

DROP TABLE IF EXISTS `_ipfx_level0_order_0`;
CREATE TABLE `_ipfx_level0_order_0` (
	`l0` INT(10) NULL,
	`i0` INT(11) NULL DEFAULT NULL,
	`cval` BIGINT(21) NOT NULL DEFAULT '0',
	`order_map` VARBINARY(3) NOT NULL DEFAULT ''
);

DROP TABLE IF EXISTS `_ipfx_level0_order_1`;
CREATE TABLE `_ipfx_level0_order_1` (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cval` BIGINT(21) NOT NULL DEFAULT '0',
	`order_map` BLOB NOT NULL,
	PRIMARY KEY (`l0`, `i0`)
);

DROP TABLE IF EXISTS `_ipfx_level0_order_2`;
CREATE TABLE `_ipfx_level0_order_2` (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cval` BIGINT(21) NOT NULL DEFAULT '0',
	`order_map` BLOB NOT NULL,
	PRIMARY KEY (`l0`, `i0`)
);

DROP TABLE IF EXISTS `_ipfx_level0_order_3`;
CREATE TABLE `_ipfx_level0_order_3` (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cval` BIGINT(21) NOT NULL DEFAULT '0',
	`order_map` BLOB NOT NULL,
	PRIMARY KEY (`l0`, `i0`)
);


DROP TABLE IF EXISTS `_ipfx_level0_order_4`;
CREATE TABLE `_ipfx_level0_order_4` (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cval` BIGINT(21) NOT NULL DEFAULT '0',
	`order_map` BLOB NOT NULL,
	PRIMARY KEY (`l0`, `i0`)
);


DROP TABLE IF EXISTS `_ipfx_level0_order_5`;
CREATE TABLE `_ipfx_level0_order_5` (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cval` BIGINT(21) NOT NULL DEFAULT '0',
	`order_map` BLOB NOT NULL,
	PRIMARY KEY (`l0`, `i0`)
);

DROP TABLE IF EXISTS `_ipfx_level0_order_6`;
CREATE TABLE `_ipfx_level0_order_6` (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cval` BIGINT(21) NOT NULL DEFAULT '0',
	`order_map` BLOB NOT NULL,
	PRIMARY KEY (`l0`, `i0`)
);

DROP TABLE IF EXISTS `_ipfx_level0_order_7`;
CREATE TABLE `_ipfx_level0_order_7` (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cval` BIGINT(21) NOT NULL DEFAULT '0',
	`order_map` BLOB NOT NULL,
	PRIMARY KEY (`l0`, `i0`)
);


DROP TABLE IF EXISTS `_ipfx_level0_order_8`;
CREATE TABLE `_ipfx_level0_order_8` (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cval` BIGINT(21) NOT NULL DEFAULT '0',
	`order_map` BLOB NOT NULL,
	PRIMARY KEY (`l0`, `i0`)
);


DROP TABLE IF EXISTS `_ipfx_level0_order_9`;
CREATE TABLE `_ipfx_level0_order_9` (
	`l0` INT(10) NOT NULL,
	`i0` INT(11) NOT NULL,
	`base_id` BIGINT(21) NOT NULL,
	`cval` BIGINT(21) NOT NULL DEFAULT '0',
	`order_map` BLOB NOT NULL,
	PRIMARY KEY (`l0`, `i0`)
);


delete from datagraph._ipfx_level0_order_1;
insert into datagraph._ipfx_level0_order_1
Select l0, v0 ,  min(id) as base_id, count(*) as cval, group_concat(char(rank_new) order by id separator '')
from 
	(select l0, v0, id,
           ( CASE (v0)
                 WHEN @vbucket 
                 THEN @vcurRow := @vcurRow + 1 
                 ELSE @vcurRow := 1 AND @vbucket := v0 
    			END
           )  AS rank_new, @vbucket, pegel
	 from 
	 	(select dim0.`level` as l0, _ipfx_d0rf(timed) div dim0.factor as v0, id, pegel 
		 from pegel_andelfingen2, _ipfx_dim0 AS dim0
		 where dim0.`level` = 1 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

delete from datagraph._ipfx_level0_order_2;
insert into datagraph._ipfx_level0_order_2
Select l0, v0 , min(id) as base_id, count(*) as cval, group_concat(char(rank_new) order by id separator '')
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
		 where dim0.`level` = 2 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

delete from datagraph._ipfx_level0_order_3;
insert into datagraph._ipfx_level0_order_3
Select l0, v0 , min(id) as base_id, count(*) as cval, group_concat(char(rank_new) order by id separator '')
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
		 where dim0.`level` = 3 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

delete from datagraph._ipfx_level0_order_4;
insert into datagraph._ipfx_level0_order_4
Select l0, v0 , min(id) as base_id, count(*) as cval, group_concat(concat(char(rank_new div 256),char(rank_new mod 256)) order by id separator '')
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
		 where dim0.`level` = 4 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

delete from datagraph._ipfx_level0_order_5;
insert into datagraph._ipfx_level0_order_5
Select l0, v0 , min(id) as base_id, count(*) as cval, group_concat(concat(char(rank_new div 256),char(rank_new mod 256)) order by id separator '')
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
		 where dim0.`level` = 5 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

delete from datagraph._ipfx_level0_order_6;
insert into datagraph._ipfx_level0_order_6
Select l0, v0 , min(id) as base_id, count(*) as cval, group_concat(concat(char(rank_new div 256),char(rank_new mod 256)) order by id separator '')
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
		 where dim0.`level` = 6 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

delete from datagraph._ipfx_level0_order_7;
insert into datagraph._ipfx_level0_order_7
Select l0, v0 , min(id) as base_id, count(*) as cval, group_concat(concat(char(rank_new div 256),char(rank_new mod 256)) order by id separator '')
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
		 where dim0.`level` = 7 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

delete from datagraph._ipfx_level0_order_8;
insert into datagraph._ipfx_level0_order_8
Select l0, v0 , min(id) as base_id, count(*) as cval, group_concat(concat(char(rank_new div 65536), char(rank_new div 256),char(rank_new mod 256)) order by id separator '')
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
		 where dim0.`level` = 8 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

delete from datagraph._ipfx_level0_order_9;
insert into datagraph._ipfx_level0_order_9
Select l0, v0 , min(id) as base_id, count(*) as cval, group_concat(concat(char(rank_new div 65536), char(rank_new div 256),char(rank_new mod 256)) order by id separator '')
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
		 where dim0.`level` = 9 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

delete from datagraph._ipfx_level0_order_10;
insert into datagraph._ipfx_level0_order_10
Select l0, v0 , min(id) as base_id, count(*) as cval, group_concat(concat(char(rank_new div 65536), char(rank_new div 256),char(rank_new mod 256)) order by id separator '')
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
		 where dim0.`level` = 10 ) B, 
	  	(SELECT @vcurRow := 0, @vbucket := 0, @vccount :=0) r
	 order by v0 asc, B.PEGEL asc ) O
group by l0,v0;

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