# -*- Mode: Python -*-

import struct
import coro
import spec
import sys

from pprint import pprint as pp
is_a = isinstance

W = sys.stderr.write

class ProtocolError (Exception):
    pass
class UnexpectedClose (Exception):
    pass

def dump_ob (ob):
    W ('%s {\n' % (ob._name,))
    for name in ob.__slots__:
        W ('  %s = %r\n' % (name, getattr (ob, name)))
    W ('}\n')

class amqp_client:

    version = [0,0,9,1]
    buffer_size = 4000

    def __init__ (self, auth, host, port=5672, virtual_host='/'):
        self.port = port
        self.host = host
        self.auth = auth
        self.virtual_host = virtual_host
        self.frame_state = 0
        self.frames = coro.fifo()
        # collect body parts here.  heh.
        self.body = []
        self.next_content_consumer = None
        self.consumers = {}

    # state diagram for connection objects:
    #
    # connection          = open-connection *use-connection close-connection
    # open-connection     = C:protocol-header
    #                       S:START C:START-OK
    #                       *challenge
    #                       S:TUNE C:TUNE-OK
    #                       C:OPEN S:OPEN-OK
    # challenge           = S:SECURE C:SECURE-OK
    # use-connection      = *channel
    # close-connection    = C:CLOSE S:CLOSE-OK
    #                     / S:CLOSE C:CLOSE-OK

    def go (self):
        self.s = coro.tcp_sock()
        self.s.connect ((self.host, self.port))
        self.s.send ('AMQP' + struct.pack ('>BBBB', *self.version))
        self.buffer = self.s.recv (self.buffer_size)
        if self.buffer.startswith ('AMQP'):
            # server rejection
            raise AMQPError (
                "version mismatch: server wants %r" % (
                    struct.unpack ('>4B', self.buffer[4:8])
                    )
                )
        else:
            coro.spawn (self.recv_loop)
            # pull the first frame off (should be connection.start)
            ftype, channel, frame = self.expect_frame (spec.FRAME_METHOD, 'connection.start')
            #W ('connection start\n')
            #dump_ob (frame)
            mechanisms = frame.mechanisms.split()
            if 'PLAIN' in mechanisms:
                response = '\x00%s\x00%s' % self.auth
            else:
                raise AuthenticationError ("no shared auth mechanism: %r" % (mechanisms,))
            reply = spec.connection.start_ok (
                # XXX put real stuff in here...
                client_properties={'product':'AMQP/shrapnel'},
                response=response
                )
            self.send_frame (spec.FRAME_METHOD, 0, reply)
            # XXX
            # XXX according to the 'grammar' from the spec, we might get a 'challenge' frame here.
            # XXX
            ftype, channel, frame = self.expect_frame (spec.FRAME_METHOD, 'connection.tune')
            self.tune = frame
            # I'm AMQP, and I approve this tune value.
            self.send_frame (
                spec.FRAME_METHOD, 0,
                spec.connection.tune_ok (frame.channel_max, frame.frame_max, frame.heartbeat)
                )
            # ok, ready to 'open' the connection.
            self.send_frame (
                spec.FRAME_METHOD, 0,
                spec.connection.open (self.virtual_host)
                )
            ftype, channel, frame = self.expect_frame (spec.FRAME_METHOD, 'connection.open_ok')

    def expect_frame (self, ftype, *names):
        ftype, channel, frame = self.frames.pop()
        if frame._name not in names:
            #W ('unexpected frame:\n')
            #dump_ob (frame)
            raise ProtocolError ("expected %r frame, got %r" % (names, frame._name))
        else:
            return ftype, channel, frame

    def recv_loop (self):
        while 1:
            while len (self.buffer):
                self.unpack_frame()
            block = self.s.recv (self.buffer_size)
            #W ('recv_loop: %d bytes\n' % (len(block),))
            if not block:
                break
            else:
                self.buffer += block
        self.s.close()

    def unpack_frame (self):
        # unpack the frame sitting in self.buffer
        ftype, chan, size = struct.unpack ('>BHL', self.buffer[:7])
        #W ('<<< frame: ftype=%r channel=%r size=%d\n' % (ftype, chan, size))
        if ftype > 4:
            self.close()
            raise ProtocolError ("invalid type in frame: %r" % (ftype,))
        elif size + 8 <= len(self.buffer) and self.buffer[7+size] == '\xce':
            # we have the whole frame
            # <head> <payload> <end> ...
            # [++++++++++++++++++++++++]
            payload, self.buffer = self.buffer[7:7+size], self.buffer[8+size:]
        else:
            # we need to fetch more data
            # <head> <payload> <end>
            # [++++++++++][--------]
            payload = self.buffer[7:] + self.s.recv_exact (size - (len(self.buffer) - 7))
            # fetch the frame end separately
            if self.s.recv_exact (1) != '\xce':
                raise ProtocolError ("missing frame end")
            else:
                self.buffer = ''
        # -------------------------------------------
        # we have the payload, what do we do with it?
        # -------------------------------------------
        if ftype == spec.FRAME_METHOD:
            cm_id = struct.unpack ('>hh', payload[:4])
            ob = spec.method_map[cm_id]()
            ob.unpack (payload, 4)
            #W ('<<< ')
            #dump_ob (ob)
            # catch asynchronous stuff here and ship it out...
            if is_a (ob, spec.basic.deliver):
                probe = self.consumers.get ((chan, ob.consumer_tag), None)
                if probe is None:
                    W ('warning, dropping unexpected data for chan=%d consumer_tag=%r\n' % (chan, ob.consumer_tag,))
                    pp (self.consumers)
                self.next_content_consumer = probe
            else:
                self.frames.push ((ftype, chan, ob))
        elif ftype == spec.FRAME_HEADER:
            cid, weight, size, flags = struct.unpack ('>hhqh', payload[:14])
            #W ('<<< HEADER: cid=%d weight=%d size=%d flags=%x payload=%r\n' % (cid, weight, size, flags, payload))
            #W ('<<< self.buffer=%r\n' % (self.buffer,))
            self.remain = size
        elif ftype == spec.FRAME_BODY:
            #W ('<<< FRAME_BODY, len(payload)=%d\n' % (len(payload),))
            self.remain -= len (payload)
            self.body.append (payload)
            if self.remain == 0:
                if self.next_content_consumer is not None:
                    self.next_content_consumer.push (self.body)
                self.body = []
        # XXX spec.FRAME_HEARTBEAT
        else:
            # XXX close connection
            raise ProtocolError ("unhandled frame type: %r" % (ftype,))

    def send_frame (self, ftype, channel, ob):
        f = []
        if ftype == 1:
            payload = struct.pack ('>hh', *ob.id) + ob.pack()
        elif ftype == 2:
            payload = ob
        elif ftype == 3:
            payload = ob
        else:
            raise ProtocolError ("unhandled frame type: %r" % (ftype,))
        frame = struct.pack ('>BHL', ftype, channel, len (payload)) + payload + chr(spec.FRAME_END)
        r = self.s.send (frame)
        #W ('send_frame: %r %d\n' % (frame, r))

    def close (self, reply_code, reply_text, class_id, method_id):
        self.send_frame (
            spec.FRAME_METHOD, 0,
            spec.connection.close (reply_code, reply_text, class_id, method_id)
            )
        ftype, channel, frame = self.expect_frame (spec.FRAME_METHOD, 'connection.close_ok')

    def channel (self, out_of_band=''):
        chan = channel (self)
        self.send_frame (spec.FRAME_METHOD, chan.num, spec.channel.open (out_of_band))
        ftype, chan_num, frame = self.expect_frame (spec.FRAME_METHOD, 'channel.open_ok')
        assert chan_num == chan.num
        return chan

    def add_consumer (self, chan, tag):
        fifo = coro.fifo()
        self.consumers[(chan, tag)] = fifo
        return fifo

class channel:

    # state diagram for channel objects:
    #
    # channel             = open-channel *use-channel close-channel
    # open-channel        = C:OPEN S:OPEN-OK
    # use-channel         = C:FLOW S:FLOW-OK
    #                     / S:FLOW C:FLOW-OK
    #                     / functional-class
    # close-channel       = C:CLOSE S:CLOSE-OK
    #                     / S:CLOSE C:CLOSE-OK
    
    counter = 1

    def __init__ (self, conn):
        self.conn = conn
        self.num = channel.counter
        channel.counter += 1
        
    def send_frame (self, ftype, frame):
        self.conn.send_frame (ftype, self.num, frame)

    # Q:, if a method is synchronous, does that mean it is sync w.r.t. this channel only?
    # in other words, might a frame for another channel come in before we get our reply?

    # leaving off all the 'ticket' args since they appear unused/undocumented...

    def exchange_declare (self, exchange=None, type='direct', passive=False,
                          durable=False, auto_delete=False, internal=False, nowait=False, arguments={}):
        frame = spec.exchange.declare (0, exchange, type, passive, durable, auto_delete, internal, nowait, arguments)
        self.send_frame (spec.FRAME_METHOD, frame)
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'exchange.declare_ok')
        return frame

    def queue_declare (self, queue='', passive=False, durable=False,
                       exclusive=False, auto_delete=False, nowait=False, arguments={}):
        frame = spec.queue.declare (0, queue, passive, durable, exclusive, auto_delete, nowait, arguments)
        self.send_frame (spec.FRAME_METHOD, frame)
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'queue.declare_ok')
        assert channel == self.num
        return frame

    def queue_bind (self, queue='', exchange=None, routing_key='', nowait=False, arguments={}):
        frame = spec.queue.bind (0, queue, exchange, routing_key, nowait, arguments)
        self.send_frame (spec.FRAME_METHOD, frame)
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'queue.bind_ok')
        assert channel == self.num
        return frame

    def basic_consume (self, queue='', consumer_tag='', no_local=False,
                       no_ack=False, exclusive=False, nowait=False, arguments={}):
        frame = spec.basic.consume (0, queue, consumer_tag, no_local, no_ack, exclusive, nowait, arguments)
        self.send_frame (spec.FRAME_METHOD, frame)
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'basic.consume_ok')
        fifo = self.conn.add_consumer (self.num, frame.consumer_tag)
        assert channel == self.num
        return fifo

    def basic_get (self, queue='', no_ack=False):
        frame = spec.basic.get (0, queue, nowait)
        self.send_frame (spec.FRAME_METHOD, frame)
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'basic.get_ok', 'basic.empty')
        assert channel == self.num
        return frame

    def basic_publish (self, payload, exchange='', routing_key='', mandatory=False, immediate=False):
        frame = spec.basic.publish (0, exchange, routing_key, mandatory, immediate)
        self.send_frame (spec.FRAME_METHOD, frame)
        class_id = spec.basic.publish.id[0] # 60
        weight = 0
        size = len (payload)
        flags = 0
        head = struct.pack ('>hhqh', class_id, weight, size, flags)
        self.send_frame (spec.FRAME_HEADER, head)
        chunk = self.conn.tune.frame_max
        for i in range (0, size, chunk):
            self.send_frame (spec.FRAME_BODY, payload[i:i+chunk])

def t0():
    ch = c.channel()
    ch.exchange_declare (exchange='ething')
    ch.queue_declare (queue='qthing', passive=False, durable=False)
    ch.queue_bind (exchange='ething', queue='qthing', routing_key='notification')
    fifo = ch.basic_consume (queue='qthing')
    return ch, fifo

if __name__ == '__main__':
    import coro.backdoor
    c = amqp_client (('guest', 'guest'), '127.0.0.1')
    coro.spawn (c.go)
    coro.spawn (coro.backdoor.serve, unix_path='/tmp/amqp.bd')
    coro.event_loop()
