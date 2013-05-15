DELIMITER //

DROP FUNCTION IF EXISTS _ipfx_d0rf //

CREATE FUNCTION _ipfx_d0rf(v bigint) RETURNS int DETERMINISTIC
BEGIN
	RETURN ( (v - 1167606600) div 60) ;
END //
//

DELIMITER ;

DROP TABLE IF EXISTS datagraph._ipfx_dim0;
CREATE TABLE datagraph._ipfx_dim0 (level int,factor int);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(0,1);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(1,4);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(2,16);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(3,64);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(4,256);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(5,1024);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(6,4096);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(7,16384);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(8,65536);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(9,262144);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(10,1048576);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(11,4194304);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(12,16777216);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(13,67108864);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(14,268435456);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(15,1073741824);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(16,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(17,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(18,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(19,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(20,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(21,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(22,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(23,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(24,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(25,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(26,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(27,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(28,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(29,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(30,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(31,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(32,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(33,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(34,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(35,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(36,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(37,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(38,0);
INSERT INTO datagraph._ipfx_dim0  (level,factor) VALUES(39,0);

DELIMITER //

DROP FUNCTION IF EXISTS _ipfx_genKey //

CREATE FUNCTION _ipfx_genKey(l0 int,i0 int) RETURNS bigint DETERMINISTIC
BEGIN
	DECLARE start bigint;
	SET start = 0;
	RETURN ((start)*64+l0)*4194304+i0 ;
END //
//

DELIMITER ;

DROP TABLE IF EXISTS datagraph.pegel_andelfingen2_pa;
CREATE TABLE datagraph.pegel_andelfingen2_pa (
	`key` BIGINT NOT NULL,
	cval BIGINT,
	sval DOUBLE,
	minval DOUBLE,
	maxval DOUBLE,
	PRIMARY KEY (`key`));

DROP TABLE IF EXISTS datagraph._ipfx_level0;
CREATE TABLE datagraph._ipfx_level0
SELECT	_ipfx_d0rf(timed) AS i0,
	COUNT(PEGEL) AS cval,
	SUM(PEGEL) AS sval,
	MIN(PEGEL) AS minval,
	MAX(PEGEL) AS maxval
#INTO datagraph._ipfx_level0
FROM datagraph.pegel_andelfingen2
GROUP BY i0;

DROP TABLE IF EXISTS datagraph._ipfx_delta;
CREATE TABLE datagraph._ipfx_delta
SELECT 	_ipfx_genKey(l0,v0) as `key`,
	cval,
	sval,
	minval,
	maxval
#INTO datagraph._ipfx_delta
FROM	(SELECT 
		dim0.level AS l0,
		level0.i0 div dim0.factor AS v0,
		SUM(level0.cval) AS cval,
		SUM(level0.sval) AS sval,
		MIN(level0.minval) AS minval,
		MAX(level0.maxval) AS maxval
	 FROM	
		_ipfx_dim0 AS dim0,
		datagraph._ipfx_level0 AS level0
	GROUP BY l0,v0) AS siq;

INSERT INTO datagraph.pegel_andelfingen2_pa (
	SELECT * FROM datagraph._ipfx_delta
);

DROP FUNCTION _ipfx_d0rf(bigint);
DROP TABLE datagraph._ipfx_dim0;
DROP TABLE datagraph._ipfx_level0;
DROP TABLE datagraph._ipfx_delta;

DELIMITER //
CREATE DEFINER=`root`@`%` FUNCTION `_ipfx_revKeyIndex`(`key` bigint)
	RETURNS int
	LANGUAGE SQL
	DETERMINISTIC
	CONTAINS SQL
	SQL SECURITY DEFINER
	COMMENT ''
BEGIN
	RETURN `key` mod 4194304 ;
END //

CREATE DEFINER=`root`@`%` FUNCTION `_ipfx_revKeyLevel`(`key` bigint)
	RETURNS int
	LANGUAGE SQL
	DETERMINISTIC
	CONTAINS SQL
	SQL SECURITY DEFINER
	COMMENT ''
BEGIN
	RETURN `key` div 4194304 ;
END //
DELIMITER ;
