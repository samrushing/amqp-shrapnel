
AMQP/Shrapnel
======

This is an implementation of the AMQP_ protocol for Shrapnel_.

Status
------

Implements version 0.9.1 of the protocol.  The basics are there:
channels, queues, exchanges, basic_publish, basic_consume.  Tested
against RabbitMQ_.

Implementation
--------------
Most of the code is auto-generated from the RabbitMQ_ machine-readable
protocol description file.  See util/codegen.py.

Plans
-----

I plan to rewrite the wire codec in Cython_, and then have the code
generator also generate Cython.  Combined with the high performance of
shrapnel itself, this should fairly scream.

.. _Cython: http://cython.org/
.. _Shrapnel: http://github.com/ironport/shrapnel/
.. _AMQP: http://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol
.. _RabbitMQ: http://www.rabbitmq.com/
