import redis

class redisHandler:
    
    def __init__(self):
        print("redis handler init")
        
        host = "redis"
        
        port = 6379
        
        self.redis = redis.Redis(
            host=host,
            port=port
        )
    
    def set(self,key,value):
        self.redis.set(
            key,
            value
        )
    
    def get(self,key):
        return self.redis.get(key)