-- Created by: Dennis Pallett (dennis@pallett.nl)
-- July, 2014


-- add function signatures to SQL catalog
create function pa_grid(pa_gridQuery text)
returns table (
        gkey bigint, pakey bigint
    )
external name neogeo.pa_grid;

create function pa_grid_enhanced(pa_gridQuery text, sys string, tab string, ckey_col string, aggr1_col string)
returns table (
        gkey bigint, pakey bigint, aggr1 bigint
    )
external name neogeo.pa_grid_enhanced;

create function pa_grid_enhanced(pa_gridQuery text, sys string, tab string, ckey_col string, aggr1_col string, aggr2_col string)
returns table (
        gkey bigint, pakey bigint, aggr1 bigint, aggr2 bigint
    )
external name neogeo.pa_grid_enhanced;

create function pa_grid_enhanced(pa_gridQuery text, sys string, tab string, ckey_col string, aggr1_col string, aggr2_col string, aggr3_col string)
returns table (
        gkey bigint, pakey bigint, aggr1 bigint, aggr2 bigint, aggr3 bigint
    )
external name neogeo.pa_grid_enhanced;

create function pa_grid_enhanced(pa_gridQuery text, sys string, tab string, ckey_col string, aggr1_col string, aggr2_col string, aggr3_col string, aggr4_col string)
returns table (
        gkey bigint, pakey bigint, aggr1 bigint, aggr2 bigint, aggr3 bigint, aggr4 bigint
    )
external name neogeo.pa_grid_enhanced;

create function pa_grid_cell(pa_gridQuery text)
returns table (
        pakey bigint
    )
external name neogeo.pa_grid_cell;

create function byte_to_hex_bigendian (num integer) 
returns char(2)
external name neogeo.byte_to_hex_bigendian;

create function short_to_hex_bigendian (num integer) 
returns char(4)
external name neogeo.short_to_hex_bigendian;

create function int24_to_hex_bigendian (num integer) 
returns char(6)
external name neogeo.int24_to_hex_bigendian;

create function int_to_hex_bigendian (num integer) 
returns char(8)
external name neogeo.int_to_hex_bigendian;