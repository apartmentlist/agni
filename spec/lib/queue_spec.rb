require 'messenger'
require 'pry'
require 'spec_helper'

describe Messenger::Queue do

  let (:channel) { mock('channel') }
  let (:connection) { mock('connection') }
  let (:messenger) do
    mock('messenger').tap do |m|
      m.stubs(:channel).returns(channel)
      m.stubs(:connection).returns(connection)
    end
  end
  let (:channel) { mock('channel') }
  let (:exchange) { mock('exchange') }
  let (:amqp_queue) { mock('amqp_queue') }
  let (:queue_name) { 'test_queue' }
  let (:queue) do
    AMQP::Channel.stubs(:new).returns(channel)
    channel.stubs(:default_exchange).returns(exchange)
    channel.expects(:queue).times(Messenger::PRIORITY_LEVELS.size)
    Messenger::Queue.new(queue_name, messenger)
  end

  describe :initialize do
    it 'should raise an exeception if a the queue name is nil' do
      lambda { Messenger::Queue.new(nil, messenger) }.should raise_exception(ArgumentError)
    end

    it 'should construct an AMQP queue for each priority level' do
      queue
    end
  end

  describe :subscribe do
    it 'should raise an exception if the queue is already subscribed' do
      queue.expects(:subscribed?).returns(true)
      lambda { queue.subscribe(nil) }.should raise_exception(Messenger::MessengerError)
    end

    it 'should invoke subscribe for each queue' do
      queue.expects(:subscribed?).returns(false)
      mock_queues = (0..9).map do |n|
        {:queue =>
          mock("mockqueue-#{n}").tap do |q|
            q.expects(:subscribe).with(is_a(Hash))
          end,
          :priority => n
        }
      end
      queue.instance_variable_set(:@queues, mock_queues)
      test_handler = lambda {}
      queue.subscribe(test_handler)
    end
  end

  describe :unsubscribe do
    it 'should raise an exception if the queue is not subscribed' do
      queue.expects(:subscribed?).returns(false)
      lambda { queue.unsubscribe }.should raise_exception(Messenger::MessengerError)
    end
  end

  describe :subscribed? do
    it 'should return true if all consumers are subscribed' do
      mock_queues = (0..9).map do |n|
        {:queue =>
          mock("mockqueue-#{n}").tap do |q|
            consumer = mock("mockconsumer-#{n}").tap do |c|
              c.expects(:subscribed?).returns(true)
            end
            q.expects(:default_consumer).returns(consumer)
          end
        }
      end
      queue.instance_variable_set(:@queues, mock_queues)
      queue.subscribed?.should be_true
    end

    it 'should return false if one consumer is not subscribed' do
      mock_queues = (0..9).map do |n|
        {:queue =>
          mock("mockqueue-#{n}").tap do |q|
            consumer = mock("mockconsumer-#{n}").tap do |c|
              c.expects(:subscribed?).returns(n == 9 ? false : true)
            end
            q.expects(:default_consumer).returns(consumer)
          end
        }
      end
      queue.instance_variable_set(:@queues, mock_queues)
      queue.subscribed?.should be_false
    end
  end

  describe :publish do
    it 'should raise and exception if the priority is out of range' do
      lambda { queue.publish('test', 11)}.should raise_exception(ArgumentError)
    end

    it 'should publish only to the proper priority level' do
      mock_queues = (0..9).map do |n|
        {:exchange => mock("mockexchange-#{n}")}
      end
      test_priority = 7
      mock_queues[test_priority][:exchange].expects(:publish).with("Hello", is_a(Hash))
      ((0..9).to_a - [test_priority]).each do |n|
        mock_queues[n][:exchange].expects(:publish).never
      end
      queue.instance_variable_set(:@queues, mock_queues)
      queue.publish("Hello", test_priority)
    end
  end

end
