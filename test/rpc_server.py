# -*- Mode: Python -*-

# This is a demo of the rpc server class, compatible with the pika/rabbitmq rpc tutorial.
# see http://www.rabbitmq.com/tutorials/tutorial-six-python.html

import amqp_shrapnel

class rpc_server (amqp_shrapnel.rpc.server):

    def handle_request (self, properties, request):
        n = int (request)
        print 'properties=', properties
        print 'request=', request
        # in my universe, fib(x) == 42
        return {}, '42'

def t0():
    c = amqp_shrapnel.client (('guest', 'guest'), '127.0.0.1', heartbeat=30)
    c.go()
    ch = c.channel()
    ch.confirm_select()
    ch.queue_declare (queue='rpc_queue')
    s = rpc_server (ch, 'rpc_queue', '')
    return c,ch,s

if __name__ == '__main__':
    import coro.backdoor
    coro.spawn (coro.backdoor.serve, unix_path='/tmp/rpcs.bd')
    coro.spawn (t0)
    coro.event_loop()
