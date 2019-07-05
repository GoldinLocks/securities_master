import pymysql 

f = open(r"USDCAD-2017_02_02-2017_02_02.csv","r")
fString = f.read()
# Test if the files values are output and readable 
# print(fString)

# conver string to list 
fList = []
for line in fString.split('\n'):
    fList.append(line.split(','))

# print(fList)

# open connection to database 
# db = pymysql.connection("localhost", "sec_user", "1v0717U7bromine!", "securities_master")
db = pymysql.connect("YOURHOST", "USERNAME", "PASSWORD", "DB_NAME")

# prepare a cursor object using cursor() method 
cursor = db.cursor()

#Drop table if it already exists using execute() method
cursor.execute("DROP TABLE IF EXISTS USDCAD")

# Create column names from the first line in fList
TIMESTAMP = fList[0][0]; OPEN = fList[0][1]; CLOSE = fList[0][2]; VOL1 = fList[0][3]; 
VOL2 = fList[0][4]

# Create USD-CAD table // place a comma after each new column except the last 
queryCreateUSDCADTable = """CREATE TABLE USDCAD(
                            {} datetime not null, 
                            {} decimal(19,5) null,
                            {} decimal(19,5) null,
                            {} decimal(19,5) null,
                            {} decimal(19,5) null
                            )""".format(TIMESTAMP, OPEN, CLOSE, VOL1, VOL2)

cursor.execute(queryCreateUSDCADTable)

del fList[0]

# Generate multiple values from thr list to be placed in a query
rows = ''
for i in range(len(fList) - 1):
    rows += "('{}','{}','{}','{}','{}')".format(fList[i][0], fList[i][1], 
    fList[i][2], fList[i][3], fList[i][4])
    if i != len(fList) - 2:
        rows += ','

# print(rows)
# print(rows) // used to make sure the last value is not a comma 
queryInsert =  "INSERT INTO USDCAD VALUES" + rows

try:
    #  Execute the SQL command
    cursor.execute(queryInsert)
    # Commit changes to the database
    db.commit()
except: 
    # Rollback incase there is any error 
    db.rollback()

db.close