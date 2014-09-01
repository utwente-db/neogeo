CREATE TABLE geometry_columns
(
  f_table_schema character varying(256) NOT NULL,
  f_table_name character varying(256) NOT NULL,
  f_geometry_column character varying(256) NOT NULL,
  coord_dimension integer NOT NULL,
  srid integer NOT NULL,
  type character varying(30) NOT NULL,
  CONSTRAINT geometry_columns_pk PRIMARY KEY (f_table_schema, f_table_name, f_geometry_column)
);