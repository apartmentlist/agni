# Messenger

## Introduction

A Messenger is a gem that wraps around the Ruby AMQP gem that provides
a very simple API for publishing and subscribing to messages.

### Simple
You'll need only two things to make use of a Messenger:
 - The URL to an AMQP instance
 - The name of a queue that you'll be writing to and/or reading from

That's it!

If you know a lot about AMQP, you'll notice that the use cases for
this class remove the notion of channels, consumers, exchanges,
bindings, routes, route keys and other primitives used by AMQP.  This
is intentional; the goal of Messenger is to provide the simplest
useful API for messaging.  That API is subject to change to the extent
it stays in line with the original design goals.

### Each message is consumed exactly once

The Messenger is designed for a simple, but very useful, configuration
of AMQP that is designed to allow 1:1, 1:n and m:n configurations for
message passing between Ruby VMs running across many machines.

One guiding principle of this class is that each message will only
ever be consumed once.  There are use cases where that behavior is not
desirable, and for those use cases Messenger should be specialized or
another approach should be used altogether.

Another guiding principle is that messages should be durable; that is,
they should survive the restart of the AMQP infrastructure.
Similarly, messages require acknowledgment.  Messenger takes care of
this for you, but it means that if your receiving code throws an
exception or crashes before it gets a chance to process the exception
fully, it will be re-queued for later processing.

### Prioritization

One of the main features of Messenger is that it supports a form of
message prioritization.  This allows systems that experience bursty
message traffic to remain responsive by prioritizing messages that
can't wait for every message ahead of them to be processed.  See below
for examples of prioritization.

Currently, Messenger supports priority levels 0 through 9 (inclusive),
with 0 being the highest priority.  There's nothing inherent in
Messenger's architecture that limits it to 10 priority levels, but we
haven't needed more than that, yet. =)

### Example uses

One example use case of a Messenger is to deliver data between stages
of a backend application (for example, data retrieval and data
processing).  Each Ruby VM that wants to participate can instantiate a
Messenger with the same AMQP URL and all publish/subscribe to the same
queue name.

Messenger could also conceivably be used to push messages onto queues
that represent units of work that need to be performed, allowing for
an arbitrary number of requesters and workers to broker work via a
single queue.

Another use case would utilize Messenger to create a scalable,
asynchronous messaging architecture, allowing many components spanning
a dozen services to communicate exclusively via various Messenger
queues to schedule work, report status, update data, and collect
metrics.  Messenger should, even in its current humble state, support
such a use case.

## Installation

Add this line to your application's Gemfile:

    gem 'messenger'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install messenger

## Usage

If you're running an AMQP instance locally for testing, try the following.

Set up a subscriber:

    require 'messenger'
    m = Messenger::Messenger.new('amqp://localhost')
    m.subscribe('test_queue') {|m,p| printf p}

Set up a publisher (in another Ruby instance, on another machine, etc.):

    require 'messenger'
    m = Messenger::Messenger.new('amqp://localhost')

It's easy to send some messages.  If you don't specify a priority,
they will default to a priority of 4.

    1.upto(100).each{|n| m.publish("test#{n}", 'test_queue')}

It's equally easy to specify a priority.  This will add 100,000
messages evenly across all 10 priorities.  If you have e.g. a RabbitMQ
dashboard handy, you can watch as the queues get emptied in priority
order.

    1.upto(100000).each{|n| m.publish("test#{n}", 'test_queue', n%10)}

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
