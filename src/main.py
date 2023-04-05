from typing import List
from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import fastavro
import sqlite3
import logging

app = FastAPI()
PAYLOAD_THRESHOLD=1000

VALUES_DICT={
    "hired_employees":[('id',"int"),('name',"string"),('datetime',"string"),('department_id',"int"),('job_id',"int")],
    "jobs":[('id',"int"),('job',"string")],
    "departments":[('id',"int"),('department',"string")]
}
DEFAULT_VALUE_DICT={
    "int":0,
    "string":"NONE"
}
class Employee(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

class Job(BaseModel):
    id: int
    job_id: int

class Department(BaseModel):
    id: int
    department: int

class Table(BaseModel):
    table_name: str
    

def insert_payload(payload: BaseModel, table:str, fields: list[str]) -> None:
    conn = sqlite3.connect('../db/challenge.db')
    c = conn.cursor()
    warning_str=""
    if len(payload)>PAYLOAD_THRESHOLD:
        warning_str="payload is too large (>{}) ".format(str(PAYLOAD_THRESHOLD))
        logging.warning(warning_str)
        payload=payload[:PAYLOAD_THRESHOLD]
    sentence="INSERT INTO {} ({}) VALUES ({})"\
                    .format(table,
                            (", ").join(fields),
                            (", ").join(["?"]*len(fields))
                    )

    for element in payload:
        element_dict=element.dict()

        c.execute(sentence, 
                  [element_dict[field] for field in fields] )
        

    conn.commit() 
    conn.close()

    return {"message": f"{warning_str}Created {len(payload)} {table}."}


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logging.error(exc.errors())
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
    )

@app.post("/employees/")
async def create_employees(employees: List[Employee]):
    insert_payload(employees, "hired_employees", [pair[0] for pair in VALUES_DICT["hired_employees"]])

@app.post("/jobs/")
async def create_jobs(jobs: List[Job]):
    insert_payload(jobs, "jobs", [pair[0] for pair in VALUES_DICT["jobs"]])

@app.post("/departments/")
async def create_(departments: List[Department]):
    insert_payload(departments, "departments", [pair[0] for pair in VALUES_DICT["departments"]])

@app.post("/backup/")
async def create_backup():
    conn = sqlite3.connect('../db/challenge.db')
    c = conn.cursor()

    for table in VALUES_DICT.keys():
        c.execute("SELECT  * FROM {}".format(table))
        rows = c.fetchall()
        records = [{VALUES_DICT[table][0]: row[i]} for i in range (len(VALUES_DICT[table])) for row in rows]
        with open('../backup/{}/{}.avro'.format(table,table), 'wb') as f:
            fastavro.writer(f, fastavro.parse_schema({
                "type": "record",
                "name": table,
                "fields": [{"name":pair[0],"type":pair[1],"default":DEFAULT_VALUE_DICT[pair[1]]} for pair in VALUES_DICT[table]]
            }), records)
    conn.close()
    return {"result":"backup sucessful"}

@app.post("/restore/")
async def restore(table: Table):
    avro_file_path = ("../backup/{}/{}.avro".format(table["table_name"]))
    with open(avro_file_path, "rb") as avro_file:
        records = fastavro.reader(avro_file)
    conn = sqlite3.connect('../db/challenge.db')
    c = conn.cursor()

    sentence="INSERT INTO {} ({}) VALUES ({})"\
                    .format(VALUES_DICT[table["name"]], (", ").join(fields), (", ").join(["?"]*len(fields)))
    for record in records:
        c.execute(sentence, 
                  (record['id'], record['name'], record['department_id'], record['job_id']))
    
    conn.commit()
    conn.close()
    return {"message":"{} restored sucessfully".format(table["name"])}

  #  entence="INSERT INTO {} ({}) VALUES ({})"\
  #                  .format(table,
  #                          (", ").join(fields),
  #                          (", ").join(["?"]*len(fields))
  #                  )

  #  for element in payload:
  #      element_dict=element.dict()

  #      c.execute(sentence, 
#               [element_dict[field] for field in fields] )