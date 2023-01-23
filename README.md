# GlobantChallenge

## Description
This project tries to resolve the a big data migration to a new database system fulfilling the following requirements:
1. Move historic data from files in CSV format to the new database.
2. Create a Rest API service to receive new data. This service must have:
2.1. Each new transaction must fit the data dictionary rules.
2.2. Be able to insert batch transactions (1 up to 1000 rows) with one request.
2.3. Receive the data for each table in the same service.
2.4. Keep in mind the data rules for each table.
3. Create a feature to backup for each table and save it in the file system in AVRO format.
4. Create a feature to restore a certain table with its backup.

In addition, the stakeholders need some metrics to be consulted through diferent endpoints that show them:

● Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.

● List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).

## Solution´s Technologies
Those are all the technologies used for this solution:

● Python

● Flask

● SQLite

● SQLAlchemy 

● Pandas

● Docker

You can deploy the app locally on python environment or with docker.

## Funcionalities
### CSV Data migration
For this csv files migration to a new database, I used the pandas library to manipulate and read the corresponding file for each table and SQLAlchemy the python SQL toolkit for model creation and SQLite database connection.
I decide to define a endpoint to request the CSV migration:

http://127.0.0.1:5000/historicalCSV

When the process its done you can view on logs all registers charged and those that did not comply with data rules are not loaded in tables
![image](https://user-images.githubusercontent.com/32706856/213954887-c1520937-7462-489c-bed1-a02e59faffeb.png)

### CSV new data ingestion
To enter new data from .csv files, an endpoint was created allowing the upload of these files to be saved and processed:

http://127.0.0.1:5000/upload

This form also allows you to select which table the load will be made to:

![image](https://user-images.githubusercontent.com/32706856/213955303-23dc24a8-4c15-4a5a-bc5a-492c1a7bbe39.png)

### Hired Employees by Q metric
The next endpoint is created allowing the query that satisfies the next desired metric:

Number of employees hired for each job and department in 2021 divided by quarter and must be ordered alphabetically by department and job.

Endpoint: http://127.0.0.1:5000/employeesByQ

The table returned view:

![image](https://user-images.githubusercontent.com/32706856/213956026-1434d7f9-e523-4adc-8f8a-3e9e7a6cdd1f.png)

### Hired Employeees by each Department metric
The next endpoint is created allowing the query that satisfies the next desired metric:

List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).

Endpoint: http://127.0.0.1:5000/employeesByDepartment

That a view of the table:

![image](https://user-images.githubusercontent.com/32706856/213955935-f1a4b486-860e-488c-92b6-d69e604e0409.png)

![image](https://user-images.githubusercontent.com/32706856/213955980-c70c4604-b66a-4370-97db-6f96e7fb8f53.png)

### Endpoints List
Finally there are all the Endpoints the API has and we had view on this document:

http://127.0.0.1:5000/historicalCSV -> CSV data migration

http://127.0.0.1:5000/upload -> CSV new data ingestion

http://127.0.0.1:5000/employeesByQ -> Hired Employees by Q metric

http://127.0.0.1:5000/employeesByDepartment -> Hired Employeees by each Department metric
