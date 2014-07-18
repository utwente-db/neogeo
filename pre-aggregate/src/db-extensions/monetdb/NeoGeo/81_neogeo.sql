-- Created by: Dennis Pallett (dennis@pallett.nl)
-- July, 2014


-- NOTE:
-- merge the contents of this file with the existing 80_udf.sql

-- add function signatures to SQL catalog
create function pa_grid(pa_gridQuery text)
returns table (
        gkey bigint, pakey bigint
    )
external name neogeo.pa_grid;

create function pa_grid_cell(pa_gridQuery text)
returns table (
        pakey bigint
    )
external name neogeo.pa_grid_cell;