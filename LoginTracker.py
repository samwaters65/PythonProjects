# -*- coding: utf-8 -*-

############################################################
# Code designed to Track and display for visualization the #
# usage of given website product                           #
############################################################



import geocoder
import csv
import pyodbc


cnxn = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};SERVER=servername;Trusted_Connection=yes;')

cur = cnxn.cursor()

select = """
Select top 1500 q.OrgRole, q.Location, min(q.loginDate) as loginDate, replace(m.StoreAddress1, ',', '') as StoreAddress1, m.StoreCity, m.StoreState, left(m.PCode, 5) as Zip
From
(select p.OrgRole, Location = case
								when p.OrgRole='Store' Then p.StoreNumber
								when p.OrgRole='District' Then p.DistrictNumber
								when p.OrgRole='Region' Then p.DistrictNumber
								when p.OrgRole='Area' Then p.AreaNumber
								End
																		 ,z.loginDate
From(Select cast(a.loginTime as DATE) as loginDate, right(b.UserName, LEN(b.Username)-3) AS ID
from SOS_System.dbo.AspNet_LoginTracking a
inner join System.dbo.AspNetUsers b on a.userId=b.Id
inner join System.dbo.AspNetUserRoles c on a.userId=c.UserId
inner join System.dbo.AspNetRoles d on c.RoleId=d.Id
where b.[Application] = 'ApplicationName' and a.successful=1 and a.loginTime >= '2016-05-16'
Group by b.UserName, cast(a.loginTime as DATE)) z
inner join Maintenance.dbo.Employees p on z.fdID=p.EmployeeNumber
where p.OrgRole='Store') q
inner join Main.dbo.Stores v on q.Location=v.StoreNumber
inner join Maintenance.dbo.stg_Stores m on m.StoreNumber=v.StoreNumber
Group by q.OrgRole, q.Location, m.StoreAddress1, m.StoreCity, m.StoreState, left(m.PCode, 5)
order by loginDate desc
"""


rows = cur.execute(select).fetchall()

LatLong = ['OrgRole, Location, LoginDate, StoreAddress, StoreCity, StoreState, Zip, Latitude, Longitude']

a = 0
b = 0
for row in rows:
    try:
        role = row[0]
        store = row[1]
        loginDate = row[2]
        addr = str(row[3]).encode("utf-8")
        city = row[4]
        state = row[5]
        zip = row[6]
        fullAddress = str(addr) + ', ' + str(city) + ', ' + str(state) + ' ' + str(zip)
        ll = geocoder.google(fullAddress)
        x = ll.latlng
        lat = x[0]
        lng = x[1]
        LatLong.append(str(rows[a][0])+', '+str(rows[a][1])+', '+str(rows[a][2])+' '+str(rows[a][3])+', '+str(rows[a][4]) + ', ' + str(rows[a][5]) + ', ' + str(rows[a][6]) + ', ' + str(lat) + ', ' + str(lng))
        print a
        a += 1
    except IndexError:
        x = [0, 0]
        lat = x[0]
        lng = x[1]
        LatLong.append(str(rows[a][0])+', '+str(rows[a][1]) + ', ' + str(rows[a][2]) + ' ' + str(rows[a][3]) + ', ' + str(rows[a][4]) + ', ' + str(rows[a][5]) + ', ' + str(rows[a][6]) + ', ' + str(lat) + ', ' + str(lng))
        print "Error"
        a += 1
        b += 1


cur.close()
cnxn.close()

csvfile = "...StoreLogins.csv"

with open(csvfile, "w+") as output:
    writer = csv.writer(output, lineterminator='\n')
    for val in LatLong:
        writer.writerow([val])

print "Number of Errors: ", b