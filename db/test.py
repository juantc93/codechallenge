import sqlite3

conn = sqlite3.connect('challenge.db')
c = conn.cursor()

for table in ['hired_employees','jobs','departments']:
    print("***TABLE***\n{}\n".format(table))
    c.execute("SELECT * FROM {} ORDER BY ID DESC LIMIT 5".format(table))
    rows = c.fetchall()

    for row in rows:
        print(row)

conn.close()