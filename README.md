# README

This repository contains code to solve the challenge in *code_challenge.md*. Due to time constraints, It Might be possible that reader finds the solution incomplete. However it should be noted that *Challenge #1* is almost complete, at least as a protype.

## Csv to Sql migration.

To speed up the development of the prototype, Code Author decided to use a Sqlite db, which is persisted in the server that host the API. Bear in mind that migration scripts and the db itself will be stored inside the **db** folder.

To migrate the sources please execute: 
* python db/setup.py

To test if migration was sucessfull please execute, a sample from every table should be printed in the terminal: 
* python db/test.py

## Endpoint
API code is stored inside the **src** code:

To launch the server for the API, end User must move to *src* run the followind command: 
* uvicorn main:app --reload

The api have the following endpoints. 

***IMPORTANT***

User is encouraged to test endpoints through FastAPI swagger doumentation appending **/docs#/** at the end of the URL in the web browser. Then user select the endpoint and press *Try it out*.
Payload expected schema is supplied to test using other means.

### Batch Load of records

To add from 1 to 1000 records user must send a post petition with a JSON array containing the records in JSON format.

#### /employees/
payload format:

[
  {
    "id": 0,
    "name": "string",
    "datetime": "string",
    "department_id": 0,
    "job_id": 0
  }
]

#### /jobs/
payload format:
[
  {
    "id": 0,
    "job_id": 0
  }
]
#### /departments/
payload format:
[
  {
    "id": 0,
    "department": 0
  }
]

### DB avro backup

### /backup/
no expected payload format (WIP). 



### DB table restore from AVRO

### /restore/
payload format: 
    {
  "table_name": "string"
}