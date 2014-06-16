CREATE TYPE MedianGroup_Shift AS (
    group_nr smallint,
    start_time bigint,
    end_time bigint,
    start_station smallint,
    end_station smallint,
    sumshift int,
    median double precision
);