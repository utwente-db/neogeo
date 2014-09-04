# PreAggregate Tools

This Maven project is used to automatically generate a set of (binary) tools for managemant of the PreAggregate index.

## Tools
Currently this project will generate 1 tool, as discussed in the next subsection.

### Create Index Tool
This tool is used to create a PreAggregate index for an table with *n* dimensions and a measure/aggregate column. It's used as following:

>usage: create-pa-index
> -axistosplit <axis index>      index of axis to split
> -chunksize <size>              maximum size of chunk after splitting axis
> -config <file>                 PreAggregate XML config file
> -d,--database <dbname>         name of database
> -dbtype <postgresql|monetdb>   type of database
> -h,--host <host>               database host name or ip address
> -help                          prints this help message
> -p,--port <port>               port number of the database
> -password <password>           database password
> -s,--schema <schema>           schema name in the database
> -u,--user <user>               database username
> -v,--verbose                   Enable verbose output logging

This tool depends on the use of a PreAggregate XML config file which is used to define the PreAggregate index by specifying the column to aggregate,
the type of aggregate that should be done (ALL|COUNT|SUM|MIN|MAX) and the dimensions to include. A sample config file is included in the project.

## Compilation
To generate the binary tools from source the Appassembler plugin of Maven is used. Run the following command to generate the tools:

`mvn package appassembler:assemble`

After successfull completion of this command a new directory `appassembler` will have been created in the `target` directory
containing a `repo` and a `bin` directory. The `bin` directory contains the actual binaries of the tools (in both Linux/Unix and Windows version)
and the `repo` directory contains the tool dependencies. You will now be able to use the tools.