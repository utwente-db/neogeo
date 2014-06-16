CREATE TYPE MedianGroup AS (
    group_nr smallint,
    row_count integer,
    start_time int,
    end_time int,
    min_x double precision,
    max_x double precision,
    min_y double precision,
    max_y double precision,
    median double precision
);