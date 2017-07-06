# -*- coding: utf-8 -*-

##########################################
# Purpose:                               #
# Quantify the sentiment of user         #
# submitted tickets through application  #
# GUI                                    #
##########################################




import pyodbc
from textblob import TextBlob

cnxn = pyodbc.connect(r'DRIVER={SQL Server Native Client 11.0};SERVER=ServerName;DATABASE=DatabaseName;Trusted_Connection=yes;')

cur = cnxn.cursor()

select = """
    SELECT TicketNoteID, Note
    FROM dbo.TicketNotes
"""


insert = """
    INSERT INTO dbo.TicketNoteSentiment
    Select TicketNoteID=?, Sentiment = ?
"""

rows = cur.execute(select).fetchall()


for row in rows:
    try:
        ident = row[0]
        text = str(row[1]).encode("utf-8")
        blob = TextBlob(text)
        sent = blob.sentiment.polarity
        cur = cur.execute(insert, [ident, sent])
        cur.commit()
    except ValueError:
        ident = row[0]
        sent = 1000
        cur = cur.execute(insert, [ident, sent])
        cur.commit()

cur.close()
cnxn.close()
