#!/usr/bin/python3
import redis

r = redis.Redis(host='127.0.0.1', port=6379, db=0)
chaves = r.keys('*')

for chave in chaves:
    r.delete(chave)

print('Redis limpo!')
