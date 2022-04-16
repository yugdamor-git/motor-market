import sys
import datetime

class price_api():
    def __init__(self,obj_):
      print("price api started")
      self.obj_master = obj_
      self.database_call_count = 0
      self.api_call_count = 0

    def get_vehicle_price_data(self,reg_no,mileage):
      import requests
      url = "https://uk1.ukvehicledata.co.uk/api/datapackage/ValuationData?v=2&api_nullitems=1&auth_apikey=66a3af5c-3f98-4784-b5fe-c0343c23b9b0&user_tag=&key_VRM={}&key_mileage={}".format(reg_no,mileage)
      response = requests.get(url)
      return response.json()


    def get_DealerForecourt_from_response(self,json_data):
      price = json_data['Response']['DataItems']['ValuationList']['DealerForecourt']
      return price

    def get_date_from_string(self,date_str):
      date = datetime.datetime.strptime(date_str,"%Y-%m-%d %H:%M:%S")
      return date

    def check_response_older_than_x_days(self,updated_date):
      import datetime
      updated = self.get_date_from_string(str(updated_date))
      now = datetime.datetime.now()
      out = now - updated
      if out.days > 30:
        return True
      else:
        return False

    def check_database_DealerForecourt_response(self,reg_no,mileage,website_id):
      DealerForecourt = None
      type_api_call = None # 0 -> database call 1-> api call.
      rows = self.obj_master.obj_db.recSelect("ukvehicledata_ValuationData",{"VRM":reg_no})
      if len(rows) > 0:
        if self.check_response_older_than_x_days(rows[0]['updated_at']):
          print("new api call for price : data is 30 days older")
          new_call = self.get_vehicle_price_data(reg_no,mileage)
          price = self.get_DealerForecourt_from_response(new_call)
          updated_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
          data = {}
          data['Response'] = str(new_call)
          data['updated_at'] = updated_time
          data['DealerForecourt'] = price
          DealerForecourt = price
          self.api_call_count = self.api_call_count + 1
          self.obj_master.obj_db.recUpdate("ukvehicledata_ValuationData",data,{"id":rows[0]['id']})
        else:
          print("getting data from db : data is  available in database")
          DealerForecourt = rows[0]['DealerForecourt']
          self.database_call_count = self.database_call_count + 1
      else:
        print("new api call for price : data is not available in database")
        self.api_call_count = self.api_call_count + 1
        new_call = self.get_vehicle_price_data(reg_no,mileage)
        price = self.get_DealerForecourt_from_response(new_call)
        updated_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data = {}
        data['Response'] = str(new_call)
        data['updated_at'] = updated_time
        data['DealerForecourt'] = price
        data['created_at'] = updated_time
        data['VRM'] = reg_no
        data['mileage'] = mileage
        data['Website_ID'] = website_id
        DealerForecourt = price
        self.obj_master.obj_db.recInsert("ukvehicledata_ValuationData",data)
      return DealerForecourt