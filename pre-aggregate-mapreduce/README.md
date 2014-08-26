# PreAggregate Index Creation with MapReduce

This directory contains all the code and utilities necessary to create the PreAggregate index using MapReduce/Hadoop instead of the standard sequantial (purely SQL-based) method.

## Usage
Creating the PreAggregate index using MapReduce consists of 3 distinct phases: PREPARE, RUN and FINISH. Each phase is discussed in the following sub-sections.

### PREPARE phase
The prepare phase splits the dataset, which must already be located in a database table, into *n* distinct chunks and puts these chunks onto the HDFS filesystem of your Hadoop cluster. This phase can be executed with the following command:

`hadoop jar neogeo-mapreduce-0.0.1-SNAPSHOT-jar-with-dependencies.jar prepare <database.properties> <preaggregate.config.xml> <hdfs_job_path> <axis_to_split> <chunksize>`

This command has the following options:

- **database.properties:** specifies the path to a properties file containing the details of your database instance. See database.example.properties for an example of this file.
- **preaggregate.config.xml:** specifies the path the PreAggregateConfiguration XML file that specifies the details of the PreAggregate index that should be created. See preaggregate.config.sample.xml for an example of this file.
- **hdfs_job_path:** specifies the path on your HDFS filesystem where all the data of the index creation will be stored. This directory will be automatically created by the PREPARE utility.
- **axis_to_split:** specifies on which axis the dataset will be split. Must be in the form of the index identifier of the example, e.g. 0 or 1 or higher depending on the number of indexes you have specified in the PreAggregate config.
- **chunksize:** specifies the size of each chunk. A lower value will result in more chunks.

After successfull completion of this phase the full dataset will be loaded onto the HDFS filesystem into *n* chunks and be ready for processing. 

### RUN phase
The run phase is a standard MapReduce job and creates the actual PreAggregate index. This phase can be executed with the following command:

`hadoop jar "neogeo-mapreduce-0.0.1-SNAPSHOT-jar-with-dependencies.jar" run <preaggregate.config.xml> <hdfs_job_path>`

This command has the following options:

- **preaggregate.config.xml:** specifies the path the PreAggregateConfiguration XML file that specifies the details of the PreAggregate index that should be created. See preaggregate.config.sample.xml for an example of this file.
- **hdfs_job_path:** specifies the path on your HDFS filesystem where all the data of the index creation will be stored. This directory will be automatically created by the PREPARE utility.

After succesfull completion of this phase the index will have been created and will be located (in possibly several parts) on the HDFS filesystem, ready to be located back into your database.

### FINISH phase
The finish phase loads the PreAggregate index back into your database. It can be executed with the following command:

`hadoop jar neogeo-mapreduce-0.0.1-SNAPSHOT-jar-with-dependencies.jar finish <database.properties> <hdfs_job_path> [-delete-job]

This command has the following options:

- **database.properties:** specifies the path to a properties file containing the details of your database instance. See database.example.properties for an example of this file.
- **hdfs_job_path:** specifies the path on your HDFS filesystem where all the data of the index creation will be stored. This directory will be automatically created by the PREPARE utility.
- **-delete-job:** *(Optional)* this switch is used to indicate that all the job data must be deleted from the HDFS filesystem

After succesfull completeion of this phase the PreAggregate index will be properly loaded back into your database and will also have been registered with the PreAggregate repository. You can now use it for PreAggregate grid/cell queries.

## JAR compilation
TODO: write how to proper compile JAR for use with Hadoop from source. Not sure yet how this fully works.

## Further notes
None at this moment.