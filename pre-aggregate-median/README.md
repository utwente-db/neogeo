# Median Pre-Aggregate Code

This directory contains the Median pre-aggregate prototype implementations, as part of the MSc research of Dennis Pallett into improving query performance of median aggregation queries.

The various prototypes are contained in the *PL-Java extensions* sub-directory. Most implementations require the use of pre-computed structures, which are generated beforehand. The tool for this can be found in the *GenerateStructure* sub-directory. Finally a basic implementation of the median aggregation function can be found in the *C* sub-directory.

## Using the prototypes

To actually use the prototype implementations follow these steps:

1. Make sure your version of PostgreSQL has the PL/Java extension installed

2. Compile the prototype you want to use by navigating to its directory and running the following command: mvn install

3. A jar should now have been created in the 'target' sub-directory. Locate the jar

4. Run the following SQL query (e.g. via pgAdmin) to install the prototype extension: SELECT sqlj.install_jar('file:///C:\Users\path\to\fastshiftmedian-0.1.jar', 'fastshiftmedian', TRUE);

5. You **may** need to install the correct type first. These are located in the 'Types' sub-directory of the PL-java extensions sub-directory.

6. Enable the extension with the following SQL query: SELECT sqlj.set_classpath('public', 'fastshiftmedian');

7. Pre-compute the correct data structure beforehand. These are specific for certain data sets.

8. You should now be able to use the extension, for example: SELECT PEGEL_MEDIAN('pegel_10k', 1);