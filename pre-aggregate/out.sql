CREATE OR REPLACE FUNCTION _ipfx_d0rf(v double precision) RETURNS integer AS $$
BEGIN
	RETURN FLOOR((v - -0.11900000000000001) / 0.0010);
END
$$ LANGUAGE plpgsql;

DROP TABLE IF EXISTS public._ipfx_dim0;
CREATE TABLE public._ipfx_dim0 (level int,factor int);
INSERT INTO public._ipfx_dim0  (level,factor) VALUES(0,1);
INSERT INTO public._ipfx_dim0  (level,factor) VALUES(1,4);
INSERT INTO public._ipfx_dim0  (level,factor) VALUES(2,16);
INSERT INTO public._ipfx_dim0  (level,factor) VALUES(3,64);
INSERT INTO public._ipfx_dim0  (level,factor) VALUES(4,256);

CREATE OR REPLACE FUNCTION _ipfx_d1rf(v double precision) RETURNS integer AS $$
BEGIN
	RETURN FLOOR((v - 51.328) / 0.0010);
END
$$ LANGUAGE plpgsql;

DROP TABLE IF EXISTS public._ipfx_dim1;
CREATE TABLE public._ipfx_dim1 (level int,factor int);
INSERT INTO public._ipfx_dim1  (level,factor) VALUES(0,1);
INSERT INTO public._ipfx_dim1  (level,factor) VALUES(1,4);
INSERT INTO public._ipfx_dim1  (level,factor) VALUES(2,16);
INSERT INTO public._ipfx_dim1  (level,factor) VALUES(3,64);
INSERT INTO public._ipfx_dim1  (level,factor) VALUES(4,256);

CREATE OR REPLACE FUNCTION _ipfx_genKey(l0 numeric,i0 numeric,l1 numeric,i1 numeric) RETURNS bigint AS $$
DECLARE start bigint;
BEGIN
	start := 0;
	RETURN ((((start)*8+l0)*1024+i0)*8+l1)*512+i1;
END
$$ LANGUAGE plpgsql;

-- create the table containg the pre aggregate index
DROP TABLE IF EXISTS public.london_hav_neogeo_pa;
CREATE TABLE public.london_hav_neogeo_pa (
	ckey bigint NOT NULL PRIMARY KEY,
	countAggr bigint,
	sumAggr bigint,
	minAggr bigint,
	maxAggr bigint
);

-- adding increment 0 to pa index
DROP TABLE IF EXISTS public._ipfx_level0;
SELECT	_ipfx_d0rf(ST_X(coordinates)) AS i0,
	_ipfx_d1rf(ST_Y(coordinates)) AS i1,
	COUNT(char_length(tweet)) AS countAggr,
	SUM(char_length(tweet)) AS sumAggr,
	MIN(char_length(tweet)) AS minAggr,
	MAX(char_length(tweet)) AS maxAggr
INTO public._ipfx_level0
FROM public.london_hav_neogeo
WHERE (ST_X(coordinates)>=-0.11900000000000001 AND ST_X(coordinates)<0.16400000000000003)
GROUP BY i0,i1;

DROP TABLE IF EXISTS public._ipfx_0_n;
SELECT 	_ipfx_genKey(l0,v0,l1,v1) as ckey,
	countAggr,
	sumAggr,
	minAggr,
	maxAggr
INTO public._ipfx_0_n
FROM	(SELECT 
		dim0.level AS l0,
		DIV(level0.i0,dim0.factor) AS v0,
		dim1.level AS l1,
		DIV(level0.i1,dim1.factor) AS v1,
		SUM(level0.countAggr) AS countAggr,
		SUM(level0.sumAggr) AS sumAggr,
		MIN(level0.minAggr) AS minAggr,
		MAX(level0.maxAggr) AS maxAggr
	 FROM	
		_ipfx_dim0 AS dim0,
		_ipfx_dim1 AS dim1,
		public._ipfx_level0 AS level0
	GROUP BY l0,v0,l1,v1) AS siq;

UPDATE public.london_hav_neogeo_pa AS pa_table 
	SET countAggr = pa_table.countAggr + pa_delta.countAggr,sumAggr = pa_table.sumAggr + pa_delta.sumAggr,minAggr = LEAST(pa_table.minAggr,pa_delta.minAggr),maxAggr = GREATEST(pa_table.maxAggr,pa_delta.maxAggr) 
	FROM public._ipfx_0_n AS pa_delta WHERE pa_delta.ckey = pa_table.ckey;

INSERT INTO public.london_hav_neogeo_pa (
	SELECT * FROM public._ipfx_0_n AS pa_delta 
	WHERE NOT EXISTS (SELECT * FROM public.london_hav_neogeo_pa AS pa_table WHERE pa_delta.ckey = pa_table.ckey));

-- adding increment 1 to pa index
DROP TABLE IF EXISTS public._ipfx_level0;
SELECT	_ipfx_d0rf(ST_X(coordinates)) AS i0,
	_ipfx_d1rf(ST_Y(coordinates)) AS i1,
	COUNT(char_length(tweet)) AS countAggr,
	SUM(char_length(tweet)) AS sumAggr,
	MIN(char_length(tweet)) AS minAggr,
	MAX(char_length(tweet)) AS maxAggr
INTO public._ipfx_level0
FROM public.london_hav_neogeo
WHERE (ST_X(coordinates)>=0.16400000000000003 AND ST_X(coordinates)<0.448)
GROUP BY i0,i1;

DROP TABLE IF EXISTS public._ipfx_0_n;
SELECT 	_ipfx_genKey(l0,v0,l1,v1) as ckey,
	countAggr,
	sumAggr,
	minAggr,
	maxAggr
INTO public._ipfx_0_n
FROM	(SELECT 
		dim0.level AS l0,
		DIV(level0.i0,dim0.factor) AS v0,
		dim1.level AS l1,
		DIV(level0.i1,dim1.factor) AS v1,
		SUM(level0.countAggr) AS countAggr,
		SUM(level0.sumAggr) AS sumAggr,
		MIN(level0.minAggr) AS minAggr,
		MAX(level0.maxAggr) AS maxAggr
	 FROM	
		_ipfx_dim0 AS dim0,
		_ipfx_dim1 AS dim1,
		public._ipfx_level0 AS level0
	GROUP BY l0,v0,l1,v1) AS siq;

UPDATE public.london_hav_neogeo_pa AS pa_table 
	SET countAggr = pa_table.countAggr + pa_delta.countAggr,sumAggr = pa_table.sumAggr + pa_delta.sumAggr,minAggr = LEAST(pa_table.minAggr,pa_delta.minAggr),maxAggr = GREATEST(pa_table.maxAggr,pa_delta.maxAggr) 
	FROM public._ipfx_0_n AS pa_delta WHERE pa_delta.ckey = pa_table.ckey;

INSERT INTO public.london_hav_neogeo_pa (
	SELECT * FROM public._ipfx_0_n AS pa_delta 
	WHERE NOT EXISTS (SELECT * FROM public.london_hav_neogeo_pa AS pa_table WHERE pa_delta.ckey = pa_table.ckey));


DROP FUNCTION _ipfx_d0rf(double precision);
DROP TABLE public._ipfx_dim0;
DROP FUNCTION _ipfx_d1rf(double precision);
DROP TABLE public._ipfx_dim1;
DROP TABLE public._ipfx_level0;
DROP TABLE public._ipfx_0_n;
