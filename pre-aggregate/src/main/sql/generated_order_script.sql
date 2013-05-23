DELIMITER //
DROP FUNCTION IF EXISTS _ipfx_longblobToInt //
CREATE DEFINER=`%`@`%` FUNCTION `_ipfx_longblobToInt`(`_bin` binary(2))
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
DROP TABLE IF EXISTS pegel_andelfingen2_pa_order;
CREATE TABLE pegel_andelfingen2_pa_order (
    `base_id` BIGINT(21) NOT NULL,
    `cnt` INT(11) NOT NULL DEFAULT '0',
    `median_start` LONGBLOB NULL,
    `median_end` LONGBLOB NULL,
    `order_map` LONGBLOB NULL);
SET SESSION group_concat_max_len = 4*2376930;
SET SESSION max_allowed_packet =4*2376930;
delete from datagraph.pegel_andelfingen2_pa_order;
insert into datagraph.pegel_andelfingen2_pa_order (base_id,cnt,order_map)
Select min(id) as base_id, count(*) as cnt, 
    group_concat(concat(
              char((rank_new  & 0xFF000000) >> 24),
              char((rank_new  & 0xFF0000) >> 16),
              char((rank_new  & 0xFF00) >> 8),
              char(rank_new & 0xFF)) order by id separator '')
from 
   (select id,
      ( @vcurRow := @vcurRow + 1) AS rank_new, pegel
	   from 
	     (select id, pegel
		  from pegel_andelfingen2) B, 
       (SELECT @vcurRow := 0) r
    order by B.PEGEL asc ) O;
