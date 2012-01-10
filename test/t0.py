# -*- Mode: Python -*-

import amqp_shrapnel

# how to run this test:

# 1) run this script.  it will print something like this:
#   $ /usr/local/bin/python test/t0.py 
#   2: Tue Jan 10 12:18:32 2012 Backdoor started on unix socket /tmp/amqp.bd
# 2) telnet to the back door:
#   $ telnet /tmp/amqp.bd
#   [note: the full path is required, don't "cd /tmp; telnet amqp.bd" it won't work]
# 3) from the back door, do this:
#   >>> ch, f = t0()
# 4) now <ch> is a channel object, and <f> is a fifo.  now we can wait for a message
#    on the fifo:
#
#   >>> f.pop()
#  
#    this will block until a message shows up...
#
# 5) Now, in another window run either t1.py (uses amqplib) or t2.py (uses this library):
#
#   $ python test/t2.py 
#   published!
#   -1: Tue Jan 10 12:49:27 2012 Exiting...
#
#   In the telnet window you should see:
#   >>> f.pop()
#   ['howdy, there!']
#   >>>
#

def t0():
    ch = c.channel()
    ch.exchange_declare (exchange='ething')
    ch.queue_declare (queue='qthing', passive=False, durable=False)
    ch.queue_bind (exchange='ething', queue='qthing', routing_key='notification')
    fifo = ch.basic_consume (queue='qthing')
    return ch, fifo

if __name__ == '__main__':
    import coro.backdoor
    c = amqp_shrapnel.client (('guest', 'guest'), '127.0.0.1')
    coro.spawn (c.go)
    coro.spawn (coro.backdoor.serve, unix_path='/tmp/amqp.bd')
    coro.event_loop()
