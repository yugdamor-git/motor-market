
import pymysql
import re
import os

class Database:
  def __init__(self):
    self.host = os.environ.get("MYSQL_HOST","db")
    self.user = os.environ.get("MYSQL_USERNAME","root")
    self.password= os.environ.get("MYSQL_PASSWORD","9076b974c31e4678f")
    self.database = os.environ.get("MYSQL_DATABASE","motormar_flynax")
    self.charset = "utf8"
    self.cursor = None

  def disconnect (self):
    self.db.close()

  def connect (self):
    # Open database connection
    self.db = pymysql.connect(host=self.host,user=self.user,password=self.password,database=self.database,cursorclass=pymysql.cursors.DictCursor)
    # #self.db.set_character_set(self.obj_config.charset)
    self.cursor = self.db.cursor(pymysql.cursors.DictCursor)
    self.cursor.execute('SET NAMES ' + self.charset + ';')
    self.cursor.execute('SET CHARACTER SET ' + self.charset + ';')
    self.cursor.execute('SET character_set_connection=' + self.charset + ';')

  def recInsert (self,table,records):
    #conn = self.initDB()
    arr_values = []
    db_value=") VALUES ("  
    sql_qry=""

    for key in records :
      if ( records[key] != '' ):
        if isinstance(records[key],dict) and 'func' in records[key].keys():
            if sql_qry:
              sql_qry+=",`"+key+"`"
              db_value+=","+records[key]['func']
            else :
              sql_qry+="`"+key+"`"
              db_value+=records[key]['func']
        else:
            if sql_qry:
              sql_qry+=",`"+key+"`"
              db_value+=",%s"        
            else :
              sql_qry+=key+"`"
              db_value+="%s"            
            arr_values.append(records[key])
      
    
    sql_qry+=db_value+')'
    sql_qry="INSERT INTO "+table+"(`"+sql_qry    
    self.cursor.execute(sql_qry,arr_values)    
    self.db.commit()
    last_insert_id = self.cursor.lastrowid
    return last_insert_id
  
  def recSelect (self,table,where_dictionary=None,limit=None,order_by=None,order_type=None):
    #cursor = self.db.cursor(pymysql.cursors.DictCursor)
    
    sql_qry="SELECT * FROM "+table
    if where_dictionary:
        sql_qry += " WHERE "
    arr_values = []
    if where_dictionary:
      for key in where_dictionary:    
        if len(arr_values) > 0:      
          sql_qry+=" AND `"+key+"` = %s "
        else:
          sql_qry+=' `'+key+"` = %s ";        
        arr_values.append(where_dictionary[key])
    if order_by:
      sql_qry+=" ORDER BY `"+order_by+"`"
      if order_type:      
        sql_qry+= " "+order_type+" "
    if limit:
      sql_qry+=" LIMIT "+str(limit)
    #print sql_qry
    self.cursor.execute(sql_qry,arr_values)
    result = self.cursor.fetchall()
    return result

  def recCustomQuery (self,sql_qry):
    #cursor = self.db.cursor(pymysql.cursors.DictCursor)
    
    self.cursor.execute(sql_qry)    
    m = re.match(r'select ', sql_qry,re.S|re.M|re.I)
    if m:      
      result = self.cursor.fetchall()
      return result
    self.db.commit()
    return []

  def recGetCount (self,table,where_dictionary):
    #cursor = self.db.cursor(pymysql.cursors.DictCursor)
    
    sql_qry="SELECT COUNT(*) as cnt FROM "+table+" WHERE "
    arr_values = []
    for key in where_dictionary:    
      if len(arr_values) > 0:      
        sql_qry+=" AND "+key+" = %s "
      else:
        sql_qry+=key+" = %s ";        
      arr_values.append(where_dictionary[key])
    self.cursor.execute(sql_qry,arr_values)
    result = self.cursor.fetchall()
    return result[0]['cnt']

  def recUpdate (self,table,records,where_dictionary):
    #cursor = self.db.cursor(pymysql.cursors.DictCursor)
    arr_values = []
    
    rec_value_count = 0
    sql_qry = ""
    
    for key in records :
      if isinstance(records[key],dict) and 'func' in records[key].keys() :
        if sql_qry:
            sql_qry+=",`"+key+"`="+ records[key]['func']
        else:      
            sql_qry+="`"+key+"`="+ records[key]['func']          
      else:
          if sql_qry:
            sql_qry+=",`"+key+"` = %s"    
          else:      
            sql_qry+=key+" = %s"        
          arr_values.append(records[key])    
      rec_value_count += 1
    if ( rec_value_count == 0 ) :
      return 1;    
    for key in where_dictionary:  
      if((rec_value_count- len(records))>0):      
        sql_qry+=" AND `"+key+"` = %s "    
      else:      
        sql_qry+=" WHERE `"+key+"` = %s "          
      arr_values.append(where_dictionary[key])
      rec_value_count += 1

    sql_qry = "UPDATE " + table + " SET " + sql_qry
    #print sql_qry  
    self.cursor.execute(sql_qry,arr_values)  
    self.db.commit()
    #conn.close()
  def recInsertUpdate(self,table,records,where_dictionary):
      rec_count = self.recGetCount(table,where_dictionary)
      if rec_count:
        self.recUpdate(table,records,where_dictionary)
      else:
        return self.recInsert(table,records)
    
  
  def recDelete (self,table,where_dictionary):
    arr_values = []
    sql_qry = "DELETE FROM "+table
    for key in where_dictionary:
        if len(arr_values)>0:
            sql_qry += " AND " + key + "=%s"
        else:
            sql_qry += " WHERE " + key + "=%s"
        arr_values.append(where_dictionary[key])
    
    self.cursor.execute(sql_qry,arr_values)
    self.db.commit()
  
  def getCurrentTs (self):
    self.cursor.execute( "SELECT NOW( ) AS current_ts" )
    result = self.cursor.fetchall()
    return result[0]['current_ts']


if __name__ == "__main__":
  # testing - it should be done from demo server only.
  
  db = Database()
  
  db.connect()
  
  try:
    listings = db.recCustomQuery('select id from fl_listings where status="active" limit 10')
    print(listings)
  except Exception as e:
    print(f'error : {str(e)}')
  
  
  db.disconnect()