# -*- Mode: Python -*-
# companion to t0.py that uses amqp-shrapnel

import amqp_shrapnel

def t2():
    global c
    c = amqp_shrapnel.client (('guest', 'guest'), '127.0.0.1')
    c.go() # i.e., connect...
    ch = c.channel()
    ch.basic_publish ('howdy there!', exchange='ething', routing_key='notification')
    print 'published!'
    coro.sleep_relative (5)
    coro.set_exit()

import coro
coro.spawn (t2)
coro.event_loop()
