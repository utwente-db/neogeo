DROP TABLE IF EXISTS public._ipfx_level0;
SELECT	_ipfx_d0rf(ST_X(coordinates)) AS i0,
	0 AS l0,
	_ipfx_d1rf(ST_Y(coordinates)) AS i1,
	0 AS l1,
	COUNT(char_length(tweet)) AS countAggr,
	SUM(char_length(tweet)) AS sumAggr,
	MIN(char_length(tweet)) AS minAggr,
	MAX(char_length(tweet)) AS maxAggr
INTO public._ipfx_level0
FROM public.london_hav_neogeo
GROUP BY i0,i1;
