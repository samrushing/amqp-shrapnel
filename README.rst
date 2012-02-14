
AMQP/Shrapnel
======

This is an implementation of the AMQP_ protocol for Shrapnel_.

Status
------

Implements version 0.9.1 of the protocol.  The basics are there:
channels, queues, exchanges, basic_publish, basic_consume.  Tested
against RabbitMQ_.

Interesting: I noticed the official AMQP site is trying to bury evidence
of previous versions of the protocol to force people into implementing 1.0.
Wow, pretty stupid, guys.

Implementation
--------------
Most of the code is auto-generated from the RabbitMQ_ machine-readable
protocol description file.  See util/codegen.py.

Plans
-----

I plan to rewrite the wire codec in Cython_, and then have the code
generator also generate Cython.  Combined with the high performance of
shrapnel itself, this should fairly scream.

Help
----

No documentation yet, see the files in the test directory for clues on
how to use this library.

.. _Cython: http://cython.org/
.. _Shrapnel: http://github.com/ironport/shrapnel/
.. _AMQP: http://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol
.. _RabbitMQ: http://www.rabbitmq.com/
