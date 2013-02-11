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
  # One example use case of a Messenger is to deliver data between
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
    include ::LogMixin

    # Enforce durability-by-default at the queue and message level
    DEFAULT_QUEUE_OPTS = {durable: true}.freeze
    DEFAULT_MESSAGE_OPTS = {persistent: true}.freeze
    # As we move towards a priority based system, expect this be
    # adjusted for each priority level
    MESSENGER_PREFETCH_COUNT = 50

    # Unless you know what you're doing, don't call this.  Using
    # Rails.application.messenger instead.
    def initialize(amqp_url)
      if amqp_url.nil? || amqp_url.empty?
        raise ArgumentError, "AMQP url is required to create a Messenger"
      end
      # Setup logging
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
      @connection = AMQP.connect(amqp_url, connection_options)
      unless @connection
        raise MessengerError, "Unable to connect to AMQP instance at #{amqp_url}"
      end

      channel_options = {
        auto_recovery: true,
        prefetch: MESSENGER_PREFETCH_COUNT,
      }
      @channel = AMQP::Channel.new(@connection, channel_options)

      unless @channel
        raise MessengerError,
          "Unable to obtain a channel from AMQP instance at #{amqp_url}"
      end

      # Get a handle to the default exchange that we'll use for all
      # messages. The default exchange automatically binds messages
      # with a given routing key to a queue with the same name,
      # eliminating the need to create specific direct bindings for
      # each queue.
      @exchange = @channel.default_exchange
    end

    # This is not as reliable as it might seem; unless the queue was
    # created or accessed already with this instance of Messenger, it
    # will fail to pass this check (that is, it will be reported as
    # not existing and this method will return false).
    def queue_exists?(queue_name)
      @channel.queues.key?(queue_name)
    end

    # Creates a queue with the given options.  If no options are
    # provided, a default set of options will be used that makes the
    # queue save its messages to disk so that they won't be lost if
    # the AMQP service is restarted.
    #
    # @return The queue that was created
    # @raise ArgumentError if the queue name is not provided
    # @raise MessengerError if the queue has already been created with
    #        an incompatible set of options.
    def create_queue(name, options={})
      if name.nil? || name.empty?
        raise ArgumentError, 'Queue name must be present when creating a queue'
      end
      begin
        @channel.queue(name, DEFAULT_QUEUE_OPTS.merge(options))
      rescue AMQP::IncompatibleOptionsError
        raise MessengerError,
          'Queue with name #{name}, has already been created with different options!'
      end
    end

    # This method provides a safe way to create a queue only if it
    # doesn't already exist.
    #
    # @param name [String] the name of the queue that should be
    #   created
    # @return this method is intended to operate by side-effect, so
    #   the return value is undefined.
    def create_queue_if_non_existent(name, options={})
      begin
        create_queue(name, options) unless queue_exists?(name)
      rescue MessengerError
        # This can get thrown in the case of a race condition, (since
        # the AMQP server is a shared resource and there is a gap
        # between the check and the creation) and can be safely
        # ignored.
      end
    end

    # @param queue_name [String] the name of the queue for which the
    #   the message count should be fetched.
    # @return [Fixnum] the number of messages in the queue with
    #   provided queue name.  If the queue is not yet created, it will
    #   be created when this method is called.
    # @raise ArgumentError if the queue_name is not supplied
    def queue_messages(queue_name)
      if queue_name.nil? || queue_name.empty?
        raise ArgumentError, 'Queue name must be present to query it for messages'
      end
      queue = create_queue_if_non_existent(queue_name)
      queue.status do |num_msgs, num_consumers|
        num_msgs
      end
    end

    # @param queue_name [String] the name of the queue for which the
    #   the consumer count should be fetched.
    # @return [Fixnum] the number of consumers for the queue with
    #   provided queue name.  If the queue is not yet created, it will
    #   be created when this method is called.
    # @raise ArgumentError if the queue_name is not supplied
    def queue_consumers(queue_name)
      if queue_name.nil? || queue_name.empty?
        raise ArgumentError, 'Queue name must be present to query it for consumers'
      end
      queue = create_queue_if_non_existent(queue_name)
      queue.status do |num_msgs, num_consumers|
        num_consumers
      end
    end

    def publish(msg, queue_name, options={})
      create_queue_if_non_existent(queue_name)
      @exchange.publish(msg, DEFAULT_MESSAGE_OPTS.merge(options).
                        merge(:routing_key => queue_name))
    end

    # Accepts a block that it will yield to for each incoming message.
    # The block passed in to this method should accept two arguments:
    # the metadata of the message being received, as well as the
    # payload of the message.
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
    # @yield [metadata, payload] a block that handles the incoming message
    def subscribe(queue_name, options={})
      if queue_name.nil? || queue_name.empty?
        raise ArgumentError, 'Queue name must be present when receiving'
      end
      create_queue_if_non_existent(queue_name)
      queue = @channel.queues[queue_name]
      ack = options[:ack].nil? ? true : options[:ack]
      if queue.subscribed?
        raise MessengerError, "Queue #{queue_name} is already subscribed!"
      end
      queue.subscribe(:ack => true) do |metadata, payload|
        yield(metadata, payload) if block_given?
        metadata.ack if ack
      end
    end

    # Unsubscribe this messenger from the queue associated with the
    # given name.
    #
    # @raise ArgumentError if the queue name is empty
    # @raise MessengerError if the queue does not exist
    def unsubscribe(queue_name)
      if queue_name.nil? || queue_name.empty?
        raise ArgumentError, 'Cannot unsubscribe from queue without a name'
      end
      unless queue_exists?(queue_name)
        raise MessengerError,
          'Cannot unsubscribe from non-existent queue #{queue_name}'
      end

      queue = @channel.queues[queue_name]

      unless queue.subscribed?
        raise MessengerError, 'Queue #{queue_name} is not subscribed'
      end

      queue.unsubscribe
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
  # missing arguements.
  class MessengerError < StandardError
  end
end
