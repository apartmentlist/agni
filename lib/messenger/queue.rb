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
      @messenger = messenger
      @logical_queue_name = queue_name

      begin
        @queues = PRIORITY_LEVELS.map do |n|
          @messenger.channel.queue(create_queue_name(@logical_queue_name, n),
                        DEFAULT_QUEUE_OPTS.merge(options))
        end
      rescue AMQP::IncompatibleOptionsError
        raise MessengerError,
          "One of the queues needed to create #{@logical_queue_name} " +
          "(#{name}), has already been created with different options!"
      end

      # The in-memory queue we use to prioritize incoming messages of
      # different priorities
      @queue_mutex = Mutex.new
      @memory_queue = Containers::MinHeap.new
    end

    def pop
      val  = []
      @queue_mutex.synchronize {
        val = @memory_queue.pop
      }
      val
    end

    def subscribe(options={}, handler)
      ack = options[:ack].nil? ? true : options[:ack]
      handle_func = lambda do
        metadata, payload = pop
        handler[metadata, payload]
        metadata.ack
      end
      ack_func = lambda do |metadata|
        metadata.ack if ack
      end
      @queues.each_with_index do |queue, priority|
        queue.subscribe(:ack => true) do |metadata, payload|
          @memory_queue.push(priority, [metadata, payload])
          #EventMachine.defer(handle_func, ack_func)
          EventMachine.defer(handle_func)
        end
      end
    end

    def unsubscribe
      unless subscribed?
        raise MessengerError, 'Queue #{queue_name} is not subscribed'
      end
      @queues.each do |queue|
        queue.unsubscribe
      end
    end

    def subscribed?
      @queues.all?(&:subscribed?)
    end

    def publish(payload, priority=DEFAULT_PRIORITY, options={})
      queue_name = create_queue_name(@logical_queue_name, priority)
      @messenger.exchange.publish(payload, DEFAULT_MESSAGE_OPTS.merge(options).
                                  merge(:routing_key => queue_name))
    end

    def message_count
      @queues.map do |queue|
        msg_count = 0
        queue.status {|num_msgs, num_consumers| msg_count = num_msgs }
        msg_count
      end.reduce(&:+)
    end

    def consumer_count
      c_count = 0
      @queues.first.status {|num_msgs, num_consumers| c_count = consumer_count }
      c_count
    end

    private

    # Strategy for mapping a base_name and a priority to an AMQP queue
    # name
    def create_queue_name(base_name, priority)
      "#{base_name}.#{priority}"
    end

  end
end
