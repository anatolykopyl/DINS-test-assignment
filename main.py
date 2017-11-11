import csv
import datetime
import time
import MySQLdb

from anomaly import is_anomaly


result = {}
with open('raw_data.csv', 'rt', encoding="UTF-8") as csvfile:
    reader = csv.reader(csvfile, quotechar='"')
    for row in reader:
        if (row[3])[:1] == "5":
            timestamp = time.mktime(datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S,%f").timetuple())
            name = row[1] + "*" + row[2]
            if name not in result:
                result[name] = []
                result[name].append([timestamp, 1, 0])
            else:
                for item in result[name]:
                    if abs(timestamp - item[0]) < 7.5 * 60:
                        item[1] += 1
                        break
                else:
                    result[name].append([timestamp, 1, 0])

is_anomaly(result)

try:
    connection = MySQLdb.connect(host="127.0.0.1", user="user1", passwd="testserver", db="mydb")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE result ( timeframe_start TIMESTAMP, api_name TEXT, http_method TEXT, \
                    count_http_code_5xx INT, is_anomaly TINYINT(1) )")

    for name in result:
        for item in result[name]:
            cursor.execute("INSERT INTO result (timeframe_start, api_name, http_method, count_http_code_5xx, is_anomaly)"
                           "VALUES (\"{0}\", \"{1}\", \"{2}\", {3}, {4})"
                           .format(str(datetime.datetime.fromtimestamp(item[0]).strftime("%Y-%m-%d %H:%M:%S")),
                                   name.split("*")[0], name[(name.find("*")+1):], item[1], item[2]))

    connection.commit()
    cursor.close()
    connection.close()
except MySQLdb.Error as err:
    print("Connection error: {}".format(err))
