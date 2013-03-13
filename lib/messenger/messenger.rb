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

    attr_reader :connection

    # Creates a Messenger, connecting to the supplied URL.
    #
    # @param amqp_url [String] the url to the AMQP instance this
    #   Messenger should connect to.  Should begin with 'amqp://'.
    def initialize(amqp_url)
      if amqp_url.nil? || amqp_url.empty?
        raise ArgumentError, "AMQP url is required to create a Messenger"
      end
      self.configure_logs
      # Start EventMachine if needed
      unless EventMachine.reactor_running?
        @em_thread = Thread.new { EventMachine.run }
      end

      # Block until EventMachine has started
      info("Waiting for EventMachine to start")
      spin_until { EventMachine.reactor_running? }
      info("EventMachine start detected")

      unless @connection = AMQP.connect(amqp_url, DEFAULT_CONNECTION_OPTS)
        raise MessengerError, "Unable to connect to AMQP instance at #{amqp_url}"
      end

      # A hash which maps queue names to Messenger::Queue objects.
      # Tracks what queues we have access to.
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
      @queues.fetch(queue_name) do |queue_name|
        queue = Queue.new(queue_name, self, options)
        @queues[queue_name] = queue
      end
    end

    # @return [TrueClass, FalseClass] whether or not a queue with the
    #   given name is known to this Messenger instance.
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
    # name.
    #
    # One of the main uses of the options hash is to specify a message
    # priority between 0 and 9:
    #
    #    messenger.publish("Hello World", "test_queue", priority: 7)
    #
    # But the default priority is 4, so this would be published with a
    # priority of 4:
    #
    #    messenger.publish("Hello World", "test_queue")
    #
    # @param msg [String] the message to enqueue
    # @param queue_name [String] the name of the queue to publish to
    # @param options [Hash] optional -- options that will be passed to
    #   the underlying AMQP queue during publishing.
    # @option priority [FixNum] the priority of the message
    #   0(high) - 9(low)
    def publish(msg, queue_name, options={})
      sym_options = options.deep_symbolize_keys
      priority = sym_options.delete(:priority) || DEFAULT_PRIORITY
      get_queue(queue_name).publish(msg, priority, sym_options)
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
    # @return [Messenger::Queue] the queue that has been subscribed
    def subscribe(queue_name, options={}, &handler)
      if queue_name.nil? || queue_name.empty?
        raise ArgumentError, 'Queue name must be present when subscribing'
      end
      queue = get_queue(queue_name)
      if queue.subscribed?
        raise MessengerError, "Queue #{queue_name} is already subscribed!"
      end
      queue.subscribe(handler, options)
      # spin_until { queue.subscribed?  }
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

    private

   def spin_until
      while not yield
        sleep(0.1)
      end
    end

  end

end
