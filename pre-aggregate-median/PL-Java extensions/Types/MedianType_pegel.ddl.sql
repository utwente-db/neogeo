CREATE TYPE MedianGroup AS (
    group_nr smallint,
    row_count integer,
    start_time bigint,
    end_time bigint,
    median double precision
);