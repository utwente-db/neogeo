CREATE OR REPLACE FUNCTION byte_to_binary_bigendian(v_number integer)
  RETURNS bytea AS
$BODY$ 
DECLARE 
        v_textresult bytea; 
        v_temp smallint; 
        v_int smallint; 
        v_i int = 0; 
BEGIN 
        v_int = v_number; 
        v_textresult = '1'; 
        WHILE(v_i >= 0) LOOP 
                raise notice 'loop %',v_int; 
                v_temp := v_int%64; 
                v_int := v_int - v_temp; 
                v_int := v_int / 64; 
                SELECT set_byte(v_textresult,v_i,v_temp) INTO v_textresult; 
                v_i := v_i - 1; 
        END LOOP; 
        return v_textresult; 
END; 

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

-- NOTE: the following is used to split this SQL script into distinct queries. Do not change!
----- QUERY SPLIT -----

CREATE OR REPLACE FUNCTION short_to_binary_bigendian(v_number integer)
  RETURNS bytea AS
$BODY$ 
DECLARE 
        v_textresult bytea; 
        v_temp smallint; 
        v_int smallint; 
        v_i int = 1; 
BEGIN 
        v_int = v_number; 
        v_textresult = '12'; 
        WHILE(v_i >= 0) LOOP 
                raise notice 'loop %',v_int; 
                v_temp := v_int%128; 
                v_int := v_int - v_temp; 
                v_int := v_int / 128; 
                SELECT set_byte(v_textresult,v_i,v_temp) INTO v_textresult; 
                v_i := v_i - 1; 
        END LOOP; 
        return v_textresult; 
END; 
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

----- QUERY SPLIT -----

CREATE OR REPLACE FUNCTION int24_to_binary_bigendian(v_number integer)
  RETURNS bytea AS
$BODY$ 
DECLARE 
        v_textresult bytea; 
        v_temp smallint; 
        v_int smallint; 
        v_i int = 2; 
BEGIN 
        v_int = v_number; 
        v_textresult = '123'; 
        WHILE(v_i >= 0) LOOP 
                raise notice 'loop %',v_int; 
                v_temp := v_int%196; 
                v_int := v_int - v_temp; 
                v_int := v_int / 196; 
                SELECT set_byte(v_textresult,v_i,v_temp) INTO v_textresult; 
                v_i := v_i - 1; 
        END LOOP; 
        return v_textresult; 
END; 
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

----- QUERY SPLIT -----

CREATE OR REPLACE FUNCTION int_to_binary_bigendian(v_number integer)
  RETURNS bytea AS
$BODY$ 
DECLARE 
        v_textresult bytea; 
        v_temp int; 
        v_int int; 
        v_i int = 3; 
BEGIN 
        v_int = v_number; 
        v_textresult = '1234'; 
        WHILE(v_i >= 0) LOOP 
                raise notice 'loop %',v_int; 
                v_temp := v_int%256; 
                v_int := v_int - v_temp; 
                v_int := v_int / 256; 
                SELECT set_byte(v_textresult,v_i,v_temp) INTO v_textresult; 
                v_i := v_i - 1; 
        END LOOP; 
        return v_textresult; 
END; 
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;