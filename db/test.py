import sqlite3
import os
script_dir=os.path.dirname(os.path.abspath(__file__))
conn = sqlite3.connect(os.path.join(script_dir,'challenge.db'))
c = conn.cursor()

for table in ['hired_employees','jobs','departments']:
    print("***TABLE***\n{}\n".format(table))
    c.execute("SELECT * FROM {} ORDER BY ID DESC LIMIT 5".format(table))
    rows = c.fetchall()

    for row in rows:
        print(row)

conn.close()