Overview:
==========
   1) requirements
   1) Set up as an Eclipse project
   2) compile project
   
1 Requirements
===============
You need a running Postgres database and a valid user name and password. Please 
set up a database and add some data. A test database is contained in 
directory src/main/resources


2 Set up as an Eclipse project
===============================
Download the files and create an eclipse project. Then run the maven command with
the following goals:  eclipse:clean eclipse:eclipse compile
The first goal removes the eclipse configurations, then the Eclipse configurations
are derived from the pom.xml configuration file. Finally, the sources are compiled.
Close the project and open it again and then the new Eclipse project configuration 
is active.


2 compile project
==================
Eclipse is compiling the files automatically.
