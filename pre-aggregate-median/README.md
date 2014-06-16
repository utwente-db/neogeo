# Median Pre-Aggregate Code

This directory contains the Median pre-aggregate prototype implementations, as part of the MSc research of Dennis Pallett into improving query performance of median aggregation queries.

The various prototypes are contained in the *PL-Java extensions* sub-directory. Most implementations require the use of pre-computed structures, which are generated beforehand. The tool for this can be found in the *GenerateStructure* sub-directory. Finally a basic implementation of the median aggregation function can be found in the *C* sub-directory.