module Messenger
  class Queue
    include LogMixin

    # Core method responsible for catching queue name problems, like
    # nil values and empty strings.
    #
    # @param queue_name [String] the name of this queue
    # @param messenger [Messenger::Messenger] the messenger object
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
        raise MessengerError,
        "One of the queues needed to create #{@logical_queue_name} " +
          "has already been created with different options!"
      end

      # The in-memory queue we use to prioritize incoming messages of
      # different priorities
      @queue_mutex = Mutex.new
      @memory_queue = Containers::MinHeap.new
    end

    def subscribe(options={}, handler)
      ack = options[:ack].nil? ? true : options[:ack]
      handle_func = lambda do
        metadata, payload = pop
        handler[metadata, payload] if handler
        metadata.ack if ack # TODO: can this be run off the reactor thread?
      end
      # We need this callback if the message can't be acked on a defer
      # thread
      ack_func = lambda do |metadata|
        metadata.ack if ack
      end
      @queues.each do |q|
        queue = q[:queue]
        priority = q[:priority]
        queue.subscribe(:ack => true) do |metadata, payload|
          @memory_queue.push(priority, [metadata, payload])
          #EventMachine.defer(handle_func, ack_func)
          EventMachine.defer(handle_func)
        end
      end
      self
    end

    def unsubscribe
      unless subscribed?
        n        raise MessengerError, 'Queue #{queue_name} is not subscribed'
      end
      @queues.each do |q|
        q[:queue].unsubscribe
      end
    end

    def subscribed?
      @queues.map{|q| q[:queue]}.all?(&:subscribed?)
    end

    # Publishes a payload to this queue.
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

    def message_count
      @queues.map{|q| q[:queue]}.map do |queue|
        msg_count = 0
        queue.status {|num_msgs, num_consumers| msg_count = num_msgs }
        msg_count
      end.reduce(&:+)
    end

    # Returns the number of consumers of this queue.
    # @precondition This method assumes that the underlying AMQP
    #   queues will only be subscribed to by consumers using Messenger.
    # @return [FixNum] the number of consumers on this queue
    def consumer_count
      c_count = 0
      @queues.first[:queue].status {|num_msgs, num_consumers| c_count = consumer_count }
      c_count
    end

    private

    # Strategy for mapping a base_name and a priority to an AMQP queue
    # name
    def create_queue_name(base_name, priority)
      "#{base_name}.#{priority}"
    end

    # Utility method to create queue hashes.
    def create_queue(messenger, priority, options)
      name = create_queue_name(@logical_queue_name, priority)
      unless channel = AMQP::Channel.new(@connection,
                                         DEFAULT_CHANNEL_OPTS.
                                         merge({prefetch: 50}))
        raise MessengerError,
        "Unable to obtain a channel from AMQP instance at #{amqp_url}"
      end
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
    # thread-safe manner.
    def pop
      val  = []
      @queue_mutex.synchronize {
        val = @memory_queue.pop
      }
      val
    end

  end
end
