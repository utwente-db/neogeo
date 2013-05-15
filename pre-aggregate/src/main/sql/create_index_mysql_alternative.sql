DROP TABLE IF EXISTS datagraph._ipfx_level0;
CREATE TABLE datagraph._ipfx_level0
SELECT	0 as l0,
	_ipfx_d0rf(timed) AS v0,
	COUNT(PEGEL) AS cval,
	SUM(PEGEL) AS sval,
	MIN(PEGEL) AS minval,
	MAX(PEGEL) AS maxval
#INTO datagraph._ipfx_level0
FROM datagraph.pegel_andelfingen2
GROUP BY v0;

DELIMITER //
DROP PROCEDURE IF EXISTS _ipfx_fill_level0//
CREATE PROCEDURE _ipfx_fill_level0(max int)
      BEGIN
              DECLARE ll  INT;
              SET ll = 2;
              WHILE ll  < max DO
                  insert into datagraph._ipfx_level0
SELECT 
		dim0.level AS l0,
		level0.v0 div dim0.factor AS v0,
		SUM(level0.cval) AS cval,
		SUM(level0.sval) AS sval,
		MIN(level0.minval) AS minval,
		MAX(level0.maxval) AS maxval
	 FROM	
		_ipfx_dim0 AS dim0,
		datagraph._ipfx_level0 AS level0
		where level0.l0 = ll and dim0.level = ll+1
	GROUP BY l0,level0.v0 div dim0.factor;
	set ll = ll+1;
              END WHILE;
      END//
DELIMITER ;

call _ipfx_fill_level0(5);

insert into datagraph.pegel_andelfingen2_pa (
	SELECT 	_ipfx_genKey(l0,v0) as `key`,
	cval,
	sval,
	minval,
	maxval
 FROM datagraph._ipfx_level0
);