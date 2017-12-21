import pymysql
import time

fetchLimit = 1000


def getXLinksFromPreviousMonth():
    
    connection = pymysql.connect(
        "localhost", "root", "root", "scrapy", charset='utf8')
    cursor = connection.cursor()
    sql = "select distinct l.link from links l WHERE l.status =200 and l.isExternal =1 and l.firstLabel not like 'সরকা%' and YEAR(l.created_at) = YEAR(CURRENT_DATE - INTERVAL 1 MONTH) AND MONTH(l.created_at) = MONTH(CURRENT_DATE - INTERVAL 1 MONTH)";
    cursor.execute(sql)
    result = cursor.fetchall()
    inClausesVal = ','.join('"'+str(t[0]+'"') for t in result)
    
    sql = "insert into links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal) SELECT domain,firstLabel,secondLabel,title,link,200,hasData,isExternal FROM tmp_links WHERE link IN (%s)" % inClausesVal
    cursor.execute(sql)

    sql = "delete FROM tmp_links WHERE link IN (%s)" % inClausesVal
    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()      
    

def getILinksFromPreviousMonth():
    connection = pymysql.connect(
        "localhost", "root", "root", "scrapy", charset='utf8')
    cursor = connection.cursor()
    sql = "select distinct domain,link,hasData from links WHERE status=200 and isExternal=0 and YEAR(created_at) = YEAR(CURRENT_DATE - INTERVAL 1 MONTH) AND MONTH(created_at) =MONTH(CURRENT_DATE - INTERVAL 1 MONTH)";
    cursor.execute(sql)
    result = cursor.fetchall()

    for row in result:
        sql = "update tmp_links set status=200 and hasData='%s' WHERE domain='%s' and link='%s' and isExternal=0" %(row[2],row[0],row[1])
        cursor.execute(sql)
    
    
    #sql = "insert into links (domain,firstLabel,secondLabel,title,link,status,hasData,isExternal) SELECT domain,firstLabel,secondLabel,title,link,200,hasData,isExternal FROM tmp_links WHERE link IN (%s)" % inClausesVal
    #ursor.execute(sql)

    #sql = "delete FROM tmp_links WHERE link IN (%s)" % inClausesVal
    #cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()
    

getILinksFromPreviousMonth()