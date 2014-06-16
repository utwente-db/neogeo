CREATE TYPE MedianGroup_Shift AS (
    group_nr smallint,
    start_time bigint,
    end_time bigint,
    sumshift int,
    median double precision
);