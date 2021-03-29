# Aurora Dataload

-------------------------------------------

Scala-based project for running ETL `Spark` jobs configured by means of an Excel file

Spark jobs are responsible for read some raw-layer `Hive` tables, 
transform and write data on a trusted-layer Hive database

Job specifications on how to filter, transform and write data are stated on an `Excel` file
using standard `SQL` syntax

The project consists also of following submodules

- `logging` which defines logging traits 
- `excel-parser` which defines all the Excel-related logics 
- `sql-parser` which defines all the logics required for parsing SQL specifications
