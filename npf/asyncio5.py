import pymysql
import time
   

def getILinksFromPreviousMonth():
    connection = pymysql.connect(
        "localhost", "root", "root", "scrapy", charset='utf8')
    cursor = connection.cursor()

    sql = "select domain,link,hasData from links where ((status !=404 and hasData>499 and isExternal=0) or (status !=404 and isExternal=1)) and  YEAR(created_at) = YEAR(CURRENT_DATE - INTERVAL 1 MONTH) AND MONTH(created_at) = MONTH(CURRENT_DATE - INTERVAL 1 MONTH)";
    cursor.execute(sql)
    result = cursor.fetchall()

    for row in result:
        sql = "update tmp_links set status=200,crawled =1,hasData='%s' WHERE domain='%s' and link='%s' " %(row[2],row[0],row[1])
        cursor.execute(sql)
        
    sql = "insert into links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal) SELECT domain,firstLabel,secondLabel,title,link,status,hasData,isExternal FROM tmp_links WHERE crawled=1"
    cursor.execute(sql)

    sql = "delete FROM tmp_links WHERE crawled=1"
    cursor.execute(sql)
    
    connection.commit()
    cursor.close()
    connection.close()
    

getILinksFromPreviousMonth()