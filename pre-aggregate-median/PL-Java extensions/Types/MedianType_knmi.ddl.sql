CREATE TYPE MedianGroup AS (
    group_nr smallint,
    row_count integer,
    start_time int,
    end_time int,
    start_station int,
    end_station int,
    median double precision
);