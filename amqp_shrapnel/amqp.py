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

# sentinel for consumer fifos
connection_closed = 'connection closed'

class client:

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
        self.next_properties = None
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
            self.server_properties = frame.server_properties
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
        try:
            while 1:
                while len (self.buffer):
                    self.unpack_frame()
                block = self.s.recv (self.buffer_size)
                #W ('recv_loop: %d bytes\n' % (len(block),))
                if not block:
                    break
                else:
                    self.buffer += block
        finally:
            self.notify_consumers_of_close()
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
                    # XXX useful for catching deliveries from previous sessions...
                    probe = self.consumers.get (chan, None)
                    if probe is None:
                        W ('warning, dropping unexpected data for chan=%d consumer_tag=%r\n' % (chan, ob.consumer_tag,))
                    else:
                        self.next_content_consumer = (ob, probe)
                else:
                    self.next_content_consumer = (ob, probe)
            else:
                self.frames.push ((ftype, chan, ob))
        elif ftype == spec.FRAME_HEADER:
            cid, weight, size, flags = struct.unpack ('>hhqH', payload[:14])
            #W ('<<< HEADER: cid=%d weight=%d size=%d flags=%x payload=%r\n' % (cid, weight, size, flags, payload))
            #W ('<<< self.buffer=%r\n' % (self.buffer,))
            if flags:
                self.next_properties = unpack_properties (flags, payload[14:])
            else:
                self.next_properties = {}
            self.remain = size
        elif ftype == spec.FRAME_BODY:
            #W ('<<< FRAME_BODY, len(payload)=%d\n' % (len(payload),))
            self.remain -= len (payload)
            self.body.append (payload)
            if self.remain == 0:
                if self.next_content_consumer is not None:
                    ob, consumer = self.next_content_consumer
                    consumer.push ((ob, self.next_properties, self.body))
                    self.next_content_consumer = None
                    self.next_properties = None
                else:
                    W ('dropped data: %r\n' % (self.body,))
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
        "create a consumer for channel <chan> and tag <tag>"
        fifo = coro.fifo()
        self.consumers[(chan, tag)] = fifo
        return fifo

    def make_default_consumer (self, chan):
        "create a catch-all consumer for channel <chan>"
        # this is useful for catching re-deliveries
        fifo = coro.fifo()
        self.consumers[chan] = fifo
        return fifo

    def notify_consumers_of_close (self):
        for _, fifo in self.consumers.iteritems():
            fifo.push (connection_closed)

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
        if not nowait:
            ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'exchange.declare_ok')
            assert channel == self.num
            return frame

    def queue_declare (self, queue='', passive=False, durable=False,
                       exclusive=False, auto_delete=False, nowait=False, arguments={}):
        frame = spec.queue.declare (0, queue, passive, durable, exclusive, auto_delete, nowait, arguments)
        self.send_frame (spec.FRAME_METHOD, frame)
        if not nowait:
            ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'queue.declare_ok')
            assert channel == self.num
            return frame

    def queue_bind (self, queue='', exchange=None, routing_key='', nowait=False, arguments={}):
        frame = spec.queue.bind (0, queue, exchange, routing_key, nowait, arguments)
        self.send_frame (spec.FRAME_METHOD, frame)
        if not nowait:
            ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'queue.bind_ok')
            assert channel == self.num
            return frame

    def basic_consume (self, queue='', consumer_tag='', no_local=False,
                       no_ack=False, exclusive=False, arguments={}):
        # we do not allow 'nowait' since that precludes us from establishing a consumer fifo.
        frame = spec.basic.consume (0, queue, consumer_tag, no_local, no_ack, exclusive, False, arguments)
        self.send_frame (spec.FRAME_METHOD, frame)
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'basic.consume_ok')
        fifo = self.conn.add_consumer (self.num, frame.consumer_tag)
        assert channel == self.num
        return fifo

    def basic_get (self, queue='', no_ack=False):
        frame = spec.basic.get (0, queue, no_ack)
        self.send_frame (spec.FRAME_METHOD, frame)
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'basic.get_ok', 'basic.empty')
        assert channel == self.num
        return frame

    def basic_publish (self, payload, exchange='', routing_key='', mandatory=False, immediate=False, properties=None):
        frame = spec.basic.publish (0, exchange, routing_key, mandatory, immediate)
        self.send_frame (spec.FRAME_METHOD, frame)
        class_id = spec.basic.publish.id[0] # 60
        weight = 0
        size = len (payload)
        if properties:
            flags, pdata = pack_properties (properties)
        else:
            flags = 0
            pdata = ''
        head = struct.pack ('>hhqH', class_id, weight, size, flags)
        self.send_frame (spec.FRAME_HEADER, head + pdata)
        chunk = self.conn.tune.frame_max
        for i in range (0, size, chunk):
            self.send_frame (spec.FRAME_BODY, payload[i:i+chunk])

    def basic_ack (self, delivery_tag=0, multiple=False):
        frame = spec.basic.ack (delivery_tag, multiple)
        self.send_frame (spec.FRAME_METHOD, frame)

    def get_ack (self):
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'basic.ack')
        return frame

    def close (self, reply_code=0, reply_text='normal shutdown', class_id=0, method_id=0):
        frame = spec.channel.close (reply_code, reply_text, class_id, method_id)
        self.send_frame (spec.FRAME_METHOD, frame)
        ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'channel.close_ok')
        return frame

    def make_default_consumer (self):
        "create a consumer to catch unexpected deliveries"
        return self.conn.make_default_consumer (self.num)

    # rabbit mq extension
    def confirm_select (self, nowait=False):
        try:
            if self.conn.server_properties['capabilities']['publisher_confirms'] != True:
                raise ProtocolError ("server capabilities says NO to publisher_confirms")
        except KeyError:
                raise ProtocolError ("server capabilities says NO to publisher_confirms")
        else:
            frame = spec.confirm.select (nowait)
            self.send_frame (spec.FRAME_METHOD, frame)
            if not nowait:
                ftype, channel, frame = self.conn.expect_frame (spec.FRAME_METHOD, 'confirm.select_ok')
                return frame


def pack_properties (props):
    sbp = spec.basic.properties
    r = []
    flags = 0
    for k, v in props.iteritems():
        if sbp.name_map.has_key (k):
            bit, unpack, pack = sbp.name_map[k]
            r.append (pack (v))
            flags |= 1<<bit
        else:
            raise KeyError ("unknown basic property: %r" % (k,))
    return flags, ''.join (r)

def unpack_properties (flags, data):
    sbp = spec.basic.properties
    r = {}
    pos = 0
    for bit, name in sbp.bit_map.iteritems():
        if flags & 1<<bit:
            _, unpack, pack = sbp.name_map[name]
            r[name], pos = unpack (data, pos)
    return r
