# Aurora Dataload

Scala-based project for running ETL `Spark` jobs configured by means of an Excel file

Spark jobs are responsible for read some raw-layer `Hive` tables, 
transform and write data on a trusted-layer Hive database. Job specifications on 
how to filter, transform and write data are stated on an `Excel` file

The project consists also of following submodules

- `logging` which simply defines a logging trait 
- `excel-parser` which defines all the Excel-related logics 
- `sql-parser` which defines all the logics required for parsing SQL specifications stated on the Excel file
