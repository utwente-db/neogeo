CREATE TYPE __pa_gridcells AS (gkey bigint, pakey bigint, pa_bytekey text);

CREATE OR REPLACE FUNCTION pa_grid(pa_gridQuery text)
    RETURNS SETOF __pa_gridcells
    AS 'pa_grid', 'compute_pa_grid'
    LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pa_grid_cell(pa_gridQuery text)
    RETURNS SETOF bigint
    AS 'pa_grid', 'compute_pa_grid_cell'
    LANGUAGE C IMMUTABLE STRICT;