from datetime import datetime

# timestamp is number of seconds since 1970-01-01
timestamp = 1703016051130
# convert the timestamp to a datetime object in the local timezone
dt_object = datetime.fromtimestamp(round(timestamp/1000.0))
print(dt_object)
