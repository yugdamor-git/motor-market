class Config:
    def __init__(self):
      ''' Constructor for this class. '''
      # Create some member
      self.debug = False
      self.host="185.59.221.77"#"motor-market.chrvbqga0lwf.us-east-2.rds.amazonaws.com" #185.59.221.77
      self.database="motormar_flynax"
      self.user="scraper_server"
      self.password="W@tn%6YuWpQiy"#"Su=x#58&vGkDmYq{SE"
      self.port=""
      self.charset="utf8"
      self.db_socket = "/var/lib/mysql/mysql.sock"
      self.data_dir_path = "/var/www/html/data_dir"
      self.font_dir_path = '/var/www/html/fonts/'
      self.image_dir_base_path = '/var/www/html/files/'
      self.scraper_dir_path = '/var/www/html/car_scrapers/'
      self.logs_dir_path = '/var/www/html/car_scrapers/logs/'
      self.delete_img_dir = True
      # self.mac_addr = '14326795300352'
      #self.mac_addr = '214894772574917'
      self.mac_addr = '256609915076703'
      #214894772574917
      self.OLD_TOTAL_THREAD_FULL = 60
      self.OLD_TOTAL_THREAD_LESS = 40

      self.NEW_TOTAL_THREAD_FULL = 0
      self.NEW_TOTAL_THREAD_LESS = 0

      self.MAX_THREAD_COUNT_POPULATE_1 = 4  #12
      self.MAX_THREAD_COUNT_POPULATE_2 = 2  #8
      self.MAX_THREAD_COUNT_POPULATE_9 = 2  #4
