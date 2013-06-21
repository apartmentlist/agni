module Agni
  class Queue
    include LogMixin

    # Core method responsible for catching queue name problems, like
    # nil values and empty strings.
    #
    # @param queue_name [String] the name of this queue
    # @param messenger [Agni::Messenger] the messenger object
    #   with which this queue is associated
    # @param options [Hash] options that will be passed to the AMQP
    #   gem during queue creation
    def initialize(queue_name, messenger, options={})
      if queue_name.nil? || queue_name.empty?
        raise ArgumentError, 'Queue name must be present when creating a queue'
      end
      self.configure_logs
      @logical_queue_name = queue_name

      begin
        @queues = PRIORITY_LEVELS.map do |priority|
          create_queue(messenger, priority, options)
        end
      rescue AMQP::IncompatibleOptionsError
        raise AgniError,
        "One of the queues needed to create #{@logical_queue_name} " +
          "has already been created with different options!"
      end

      # The in-memory queue we use to prioritize incoming messages of
      # different priorities
      @queue_mutex = Mutex.new
      @memory_queue = Containers::MinHeap.new
    end

    # Subscribes to this queue, handling each incoming message with
    # the provided +handler+.
    # @param handler [Proc] accepts two arguments:
    #   metadata [Hash] a hash of attributes as it is provided by the
    #   underlying AMQP implementation.
    #   payload [String] the message itself, as was provided by the
    #   publisher
    #   The return value from the handler will be discarded.
    def subscribe(handler, options={})
      if subscribed?
        raise AgniError, 'Queue #{queue_name} is already subscribed'
      end
      ack = options[:ack].nil? ? true : options[:ack]
      handle_func = lambda do
        metadata, payload = pop
        handler[metadata, payload] if handler
        EventMachine.next_tick{ metadata.ack } if ack
      end
      @queues.each do |q|
        queue = q[:queue]
        priority = q[:priority]
        queue.subscribe(:ack => true) do |metadata, payload|
          @queue_mutex.synchronize do
            @memory_queue.push(priority, [metadata, payload])
          end
          EventMachine.next_tick { EventMachine.defer(handle_func) }
        end
      end
      self
    end

    def unsubscribe
      unless subscribed?
        raise AgniError, 'Queue #{queue_name} is not subscribed'
      end
      @queues.each do |q|
        q[:queue].unsubscribe
      end
    end

    # @return [True] iff every AMQP queue is +subscribed+?
    def subscribed?
      @queues.map{|q| q[:queue].default_consumer}.all?{|c| c.subscribed? if c}
    end

    # Publishes a payload to this queue.
    #
    # @param payload [String] the payload of the message to publish
    # @param priority [FixNum] must be one between 0 and 9, inclusive.
    # @param options [Hash]
    def publish(payload, priority=DEFAULT_PRIORITY, options={})
      unless PRIORITY_LEVELS.include? priority
        raise ArgumentError, "Invalid priority #{priority}, must be between 0 and 9"
      end
      queue_name = create_queue_name(@logical_queue_name, priority)
      @queues[priority][:exchange].publish(payload, DEFAULT_MESSAGE_OPTS.merge(options).
                                           merge(:routing_key => queue_name))
    end

    # Given a base name and a priority, creates a queue name suitable
    # for use in naming an underlying AMQP queue.
    #
    # @param base_name [String] the base name of the queue.  This is
    #   typcially just the queue name used when creating this
    #   +Agni::Queue+ object.
    # @param priority [String] valid priorities are in the range 0
    #   through 9 inclusive.
    def create_queue_name(base_name, priority)
      "#{base_name}.#{priority}"
    end

    def published
      @queues.reduce(0){|a,q| a += q[:channel].publisher_index}
    end

    private

    # Internal use utility method to create queue hashes.  No checking
    # is performed to ensure that the queue does not already exist,
    # for example.  Its only use right now is during initialization of
    # the Agni::Queue class.
    def create_queue(messenger, priority, options)
      name = create_queue_name(@logical_queue_name, priority)
      unless channel = AMQP::Channel.new(messenger.connection,
                                         DEFAULT_CHANNEL_OPTS.
                                         merge({prefetch: DEFAULT_PREFETCH}))
        raise AgniError,
        "Unable to obtain a channel from AMQP instance at #{amqp_url}"
      end

      # Require confirmation on message publish
      # channel.confirm_select

      # Get a handle to the default exchange. The default exchange
      # automatically binds messages with a given routing key to a
      # queue with the same name, eliminating the need to create
      # specific direct bindings for each queue.
      queue = channel.queue(name, DEFAULT_QUEUE_OPTS.
                            merge(options))

      exchange = channel.default_exchange
      # Each 'queue' in the queue array is a hash.  Here's how each
      # hash is laid out:
      {
        priority:  priority,
        name:      name,
        channel:   channel,
        queue:     queue,
        exchange:  exchange
      }
    end

    # Removes and returns an item from the priority queue in a
    # thread-safe manner.  Thread safety reasoning: all calls to
    # shared queue are locked by a single mutex.
    def pop
      val = []
      @queue_mutex.synchronize do
        val = @memory_queue.pop
      end
      val
    end

  end
end
