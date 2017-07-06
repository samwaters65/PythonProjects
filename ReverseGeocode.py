#############################
# Script to reverse geocode #
# using location's City and #
# State                     #
#############################




import geocoder
import csv

forOutput = ['StoreNumber', 'City', 'State']


GOOGLE_API_KEY = APIKey

ofile = open('...LatLongOutput.csv', "wb")
writer = csv.writer(ofile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONE, escapechar='\\')

a = 1

with open("...LatLong.csv", 'rb') as f:
    reader = csv.reader(f)
    for row in reader:
        try:
            StoreNum = int(row[0])
            Lat = float(row[1])
            Long = float(row[2])
            ll = geocoder.google([Lat, Long], method='reverse', key = GOOGLE_API_KEY)
            city = ll.city
            state = ll.state
            combo = [str(StoreNum) + ', ' + str(city) + ', ' + str(state)]
            writer.writerow(combo)
            print a
            a += 1
        except UnicodeEncodeError:
            StoreNum = row[0]
            city = 'NULL'
            state = 'NULL'
            combo = [str(StoreNum) + ', ' + str(city) + ', ' + str(state)]
            writer.writerow(combo)
            print a
            a += 1


f.close()
ofile.close()
