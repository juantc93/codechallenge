import sqlite3
import csv
import os

script_dir=os.path.dirname(os.path.abspath(__file__))
conn = sqlite3.connect(os.path.join(script_dir,'challenge.db'))
c = conn.cursor()
table_fields_dict={
    'jobs':['id','job'],
    'hired_employees':['id','name','datetime','department_id','job_id'],
     "departments":['id','department']
    }

def safe_insert(table: str, fields: list[str])-> None:
    with open(os.path.join(script_dir,'../csv_source/{}.csv'.format(table)), 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, fieldnames=fields)
        for row in reader:
            try:
                sentence="INSERT INTO {} ({}) VALUES ({})"\
                        .format(table,
                                (", ").join(fields),
                                (", ").join(["?"]*len(fields))
                        )
                c.execute(sentence,
                        [row[field] if row[field] != '' else None for field in fields
                            ])
                
            except Exception as e:
                print('Error: ', e)

try:
    c.execute('''CREATE TABLE jobs
                (id INTEGER PRIMARY KEY, job TEXT)''')
    safe_insert('jobs',table_fields_dict['jobs'])
except Exception as e:
    print('Error: ', e)  

try:
    c.execute('''CREATE TABLE departments
                (id INTEGER PRIMARY KEY, department TEXT)''')
    safe_insert('departments',table_fields_dict['departments'])
except Exception as e:
    print('Error: ', e)

try:
    c.execute('''CREATE TABLE hired_employees
             (id INTEGER PRIMARY KEY, 
             name TEXT NOT NULL, 
             datetime TEXT NOT NULL,
             department_id INTEGER NOT NULL,
             job_id INTEGER NOT NULL,
             FOREIGN KEY(department_id) REFERENCES departments(id), 
             FOREIGN KEY(job_id) REFERENCES jobs(id))''')
    safe_insert('hired_employees',table_fields_dict['hired_employees'])
except Exception as e:
    print('Error: ', e)    

conn.commit()
conn.close()
