module Messenger
  # A Messenger is a wrapper around the Ruby AMQP gem that provides a
  # very simple API for publishing and subscribing to messages.
  #
  # You'll need two things to make use of a Messenger:
  #  - The URL to an AMQP instance
  #  - The name of a queue that you'll be writing to and/or reading from
  #
  # That's it!
  #
  # The Messenger is designed for a simple, but very useful,
  # configuration of AMQP that is designed to allow 1:1, 1:n and m:n
  # configurations for message passing between Ruby VMs running across
  # many machines.
  #
  # One guiding principle of this class is that each message will only
  # ever be consumed once.  There are use cases where that behavior is
  # not desirable, and for those use cases this class should be
  # specialized or another class should be used altogether.
  #
  # Another guiding principle is that messages should be durable; that
  # is, they should survive the restart of the AMQP infrastructure.
  #
  # Onne example use case of a Messenger is to deliver data between
  # stages of a backend application (for example, data retrieval and
  # data processing).  Each Ruby VM that wants to participate can
  # instantiate a Messenger with the same AMQP url and all
  # publish/subscribe to the same queue name.
  #
  # Messenger could also conceivably be used to push messages onto
  # queues that represent units of work that need to be performed,
  # allowing for an arbitary number of requestors and workers to
  # broker work via a single queue.
  #
  # Another use case would utilize Messenger to create a scalable,
  # asynchronous messaging architecture, allowing many components
  # spanning a dozen services to communicate exclusively via various
  # Messenger queues to schedule work, report status, update data, and
  # collect metrics.  Messenger should, even in its current humble
  # state, support such a use case.
  #
  # If you know a lot about AMQP, you'll notice that the use cases for
  # this class remove the notion of channels, consumers, exchanges,
  # bindings, routes, route keys and other primitives used by AMQP.
  # This is intentional; the goal of Messenger is to provide the
  # simplest useful API for messaging.  That API is subject to change
  # to the extent it stays in line with the original design goals.
  class Messenger
    include LogMixin

    attr_reader :connection, :channel, :exchange

    # Unless you know what you're doing, don't call this.  Using
    # Rails.application.messenger instead.
    def initialize(amqp_url)
      if amqp_url.nil? || amqp_url.empty?
        raise ArgumentError, "AMQP url is required to create a Messenger"
      end
      self.configure_logs
      # It does not appear that, for any given dyno, EventMachine will
      # be running, even though our app is based on Thin (since EM is
      # single threaded).  Accordingly, we neet to start it if needed.
      unless EventMachine.reactor_running?
        @em_thread = Thread.new { EventMachine.run }
      end

      # Ensure the EventMachine has really started before we attempt
      # to connect.
      info("Waiting for EventMachine to start")
      while not EventMachine.reactor_running?
        sleep(0.1)
      end
      info("EventMachine start detected")

      # Log cases in which we're unexpectedly disconnected from the server
      connection_options = {
        auto_recovery: true,
        on_tcp_connection_failure: ->{ warning("TCP connection failure detected") }
      }

      unless @connection = AMQP.connect(amqp_url, connection_options)
        raise MessengerError, "Unable to connect to AMQP instance at #{amqp_url}"
      end

      channel_options = {
        auto_recovery: true,
        prefetch: MESSENGER_PREFETCH_COUNT,
      }

      unless @channel = AMQP::Channel.new(@connection, channel_options)
        raise MessengerError,
          "Unable to obtain a channel from AMQP instance at #{amqp_url}"
      end

      # Get a handle to the default exchange that we'll use for all
      # messages. The default exchange automatically binds messages
      # with a given routing key to a queue with the same name,
      # eliminating the need to create specific direct bindings for
      # each queue.
      @exchange = @channel.default_exchange

      # A hash of queue names to Messenger::Queue objects to keep
      # track of what queues we have access to.
      @queues = {}
    end

    # Gets a queue with the given options.  If no options are
    # provided, a default set of options will be used that makes the
    # queue save its messages to disk so that they won't be lost if
    # the AMQP service is restarted.
    #
    # @return [Messenger::Queue] the queue with the provided name
    # @raise ArgumentError if the queue name is not provided
    # @raise MessengerError if the queue has already been created with
    #        an incompatible set of options.
    def get_queue(queue_name, options={})
      # Intermediate value allows initializer for Messenger to throw
      # exceptions before we mutate the queues variable.
      unless queue = @queues[queue_name]
        queue = Queue.new(queue_name, self, options)
        @queues[queue_name] = queue
      end
      queue
    end

    def queue?(queue_name)
      @queues.key?(queue_name)
    end

    # Get and return the number of messages in a given queue.
    #
    # @param queue_name [String] the name of the queue for which the
    #   the message count should be fetched.
    # @return [Fixnum] the number of messages in the queue with
    #   provided queue name.  If the queue is not yet created, the
    #   method will return +nil+.
    # @raise ArgumentError if the queue_name is not supplied
    def queue_messages(queue_name)
      get_queue(queue_name).message_count if queue?(queue_name)
    end

    # @param queue_name [String] the name of the queue for which the
    #   the consumer count should be fetched.
    # @return [Fixnum] the number of consumers for the queue with
    #   provided queue name.  If the queue is not yet created, it will
    #   be created when this method is called.
    # @raise ArgumentError if the queue_name is not supplied
    def queue_consumers(queue_name)
      get_queue(queue_name).consumer_count if queue?(queue_name)
    end

    # Convenience method that publishes a message to the given queue
    # name, with an optional priority.
    #
    # @param msg [String] the message to enqueue
    # @param queue_name [String] the name of the queue to publish to
    # @param priority [FixNum] optional -- the priority of the message
    #   0(high) - 9(low)
    # @param options [Hash] optional -- options that will be passed to
    #   the underlying AMQP queue during publishing.
    def publish(msg, queue_name, priority=DEFAULT_PRIORITY, options={})
      get_queue(queue_name).publish(msg, priority, options)
    end

    # @note The block passed to this method must not block, since it
    #   will be run in a single-threaded context.
    #
    # Convenience method that takes a queue name (creating the queue
    # if necessary) and accepts a block that it will yield to for each
    # incoming message.  The block passed in to this method should
    # accept two arguments: the metadata of the message being
    # received, as well as the payload of the message.
    #
    # This method is non-blocking, and if at any time the Messenger
    # should no longer yield to the provided block when new messages
    # arrive, the +unsubscribe+ method can be called on the Messenger
    # and given the queue name to unsubscribe from.
    #
    # If no block is passed to this method, it will simply subscribe
    # to the queue and drain it of messages as they come in.
    #
    # To prevent lossage, this method will set up the subscription
    # with the AMQP server to require acking of the messages by the
    # client.  As far as the end user is concerned, this means that if
    # the messenger dies an untimely death, any unprocessed messages
    # that remained in the buffer will be requeued on the server.
    # Messenger will take care of the acking for the user, unless an
    # option is passed to indicate that the user will handle acking in
    # the provided block.
    #
    # @param queue_name [String] The name of the queue that should be
    #        examined for messages
    # @param options [Hash] (optional) A hash of options
    # @option options [TrueClass, FalseClass] :ack Whether messenger
    #         should ack incoming messages for this subscription.  If
    #         set to +false+, the block passed to this method must ack
    #         messages when they have been processed.  Defaults to
    #         +true+.
    # @yield handler [metadata, payload] a block that handles the incoming message
    def subscribe(queue_name, options={}, &handler)
      if queue_name.nil? || queue_name.empty?
        raise ArgumentError, 'Queue name must be present when subscribing'
      end
      queue = get_queue(queue_name)
      if queue.subscribed?
        raise MessengerError, "Queue #{queue_name} is already subscribed!"
      end
      queue.subscribe(options, handler)
    end

    # Unsubscribe this messenger from the queue associated with the
    # given name.
    #
    # @raise ArgumentError if the queue name is empty
    # @raise MessengerError if the queue does not exist
    def unsubscribe(queue_name)
      if queue_name.nil? || queue_name.empty?
        raise ArgumentError, 'Queue name must be present when unsubscribing'
      end
      if queue = get_queue(queue_name)
        queue.unsubscribe
      end
    end

    # This method allows a client of the messenger to block on the
    # execution of the EventMachine, so it can run in a context that
    # is dedicated to running for the purpose of receiving messages.
    def wait
      @em_thread.join
    end

    # Returns +true+ if the messenger is connected to AMQP, +false+
    # otherwise.
    def connected?
      return @connection.connected?
    end

  end

  # Error class used to denote error conditions specific to the
  # messenger infrastructure, and not those due to, for example,
  # missing arguments.
  class MessengerError < StandardError
  end

end
