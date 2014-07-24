# MonetDB NeoGeo extension

This is a C extension for MonetDB which adds the (necessary) pa_grid and pa_grid_cell functions, which is used by the NeoGeo pre-aggregate Java implementation.

## Installation

To install this extension follow these steps:

1. Copy the NeoGeo directory to the `sql/backends/monet5`

2. In the `sql/backends/monet5` directory of your MonetDB installation, edit the file `Makefile.ag` and add the entry `NeoGeo` to the SUBDIRS property. This ensures that the NeoGeo extension is found by the buildtools.

3. Run the bootstrap program in the main directory of your MonetDB installation to create the necessary additional Makefiles (i.e. `./bootstrap`).

4. Re-compile your MonetDB installation by completing a full cycle of configure/make/make install

5. The extension should now be installed and the functions should be auto-loaded. If this is not the case then manually load the functions by executing the SQL that is found in the file 81_neogeo.sql of the NeoGeo directory.

See [Source compilation](https://www.monetdb.org/Developers/SourceCompile) and [User Defined Functions](https://www.monetdb.org/Documentation/Cookbooks/SQLrecipes/UserDefinedFunction) for more information on compiling MonetDB.
