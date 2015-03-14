# Introduction #

This work is the implementation and benchmark of a table type to MySQL Data Base Management System (DBMS). A storage engine is the layer of MySQL that implements the persistence of the data and it communicates with the DBMS through an Application Programming Interface (API). Each table on MySQL can be owned by a different storage engine identified by the table type. The developed table type allows table creating and dropping, record selecting, inserting, deleting and updating. The table type architecture defines that each column of all records are sorted to reduce redundancy and increase performance, without creating any secondary indexes. The benchmark is implemented comparing test results of the new table type with other MySQL table types, highlighting the main differences, limitations and capabilities. The development of the table type was in C++ based on the example storage engine available in MySQL sources.
Key-words: MySQL. Table type. Storage engine.

# Details #

Download full documentation:  http://code.google.com/p/vogal/source/browse/trunk/docs/TCC%20-%20Volume%20Final.pdf.