# -*- Mode: Python -*-

import amqp_shrapnel

# how to run this test:

# 1) run this script.  it will print something like this:
#   $ /usr/local/bin/python test/t0.py
#   2: Tue Jan 10 12:18:32 2012 Backdoor started on unix socket /tmp/amqp.bd
# 2) Now, in another window run either t1.py (uses amqplib) or t2.py (uses this library):
#
#   $ python test/t2.py
#   published!
#   -1: Tue Jan 10 12:49:27 2012 Exiting...
#
#   In the first window you should see the message show up.

def t0():
    c = amqp_shrapnel.client (('guest', 'guest'), '127.0.0.1', heartbeat=30)
    c.go()
    ch = c.channel()
    ch.exchange_declare (exchange='ething')
    ch.queue_declare (queue='qthing', passive=False, durable=False)
    ch.queue_bind (exchange='ething', queue='qthing', routing_key='rpc')
    fifo = ch.basic_consume (queue='qthing')
    while 1:
        print 'waiting...'
        print fifo.pop()

if __name__ == '__main__':
    import coro.backdoor
    coro.spawn (coro.backdoor.serve, unix_path='/tmp/amqp.bd')
    coro.spawn (t0)
    coro.event_loop()
