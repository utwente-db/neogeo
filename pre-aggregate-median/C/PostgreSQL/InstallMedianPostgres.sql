CREATE OR REPLACE FUNCTION median_double_transfn(internal, double precision)
  RETURNS internal AS
'MedianExtension.so', 'median_double_transfn'
  LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION median_finalfn_double(internal)
  RETURNS double precision AS
'MedianExtension.so', 'median_finalfn_double'
  LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION median_finalfn_numeric(internal)
  RETURNS numeric AS
'MedianExtension.so', 'median_finalfn_numeric'
  LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION median_float_transfn(internal, double precision)
  RETURNS internal AS
'MedianExtension.so', 'median_float_transfn'
  LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION median_int2_transfn(internal, smallint)
  RETURNS internal AS
'MedianExtension.so', 'median_int2_transfn'
  LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION median_int4_transfn(internal, integer)
  RETURNS internal AS
'MedianExtension.so', 'median_int4_transfn'
  LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION median_int8_transfn(internal, bigint)
  RETURNS internal AS
'MedianExtension.so', 'median_int8_transfn'
  LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION median_numeric_transfn(internal, numeric)
  RETURNS internal AS
'MedianExtension.so', 'median_numeric_transfn'
  LANGUAGE c IMMUTABLE;


CREATE AGGREGATE median(double precision) (
  SFUNC = median_double_transfn,
  STYPE = internal,
  FINALFUNC = median_finalfn_double
);

CREATE AGGREGATE median(numeric) (
  SFUNC = median_numeric_transfn,
  STYPE = internal,
  FINALFUNC = median_finalfn_numeric
);