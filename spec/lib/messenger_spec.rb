require 'messenger'
require 'spec_helper'

describe Messenger::Messenger do
  let (:amqp_url) { "amqp://localhost" }
  # A messenger object using mocked AMQP methods
  let (:connection) { mock('connection') }
  let (:channel) { mock('channel') }
  let (:exchange) { mock('exchange') }
  let (:messenger) {
    EventMachine.stubs(:reactor_running?).returns(true)
    AMQP.expects(:connect).with(amqp_url, is_a(Hash)).returns(connection)
    Messenger::Messenger.new(amqp_url)
  }

  describe 'construction' do
    it 'should create a connection, channel and exchange on instantiation' do
      messenger.class.should == Messenger::Messenger
    end

    it 'should throw an exception given a blank url' do
      lambda{Messenger::Messenger.new('')}.should raise_error(ArgumentError)
    end
  end

  describe 'get_queue' do

    it 'should raise an error if the queue name is blank' do
      lambda{ messenger.get_queue('') }.should raise_error(ArgumentError)
    end

    it 'should create the queue on the channel' do
      queue_name = 'test_queue'
      Messenger::Queue.stubs(:new).with(queue_name,
                                        is_a(Messenger::Messenger),
                                        {})
      messenger.get_queue(queue_name)
    end

    it 'should not create the queue if it exists' do
      queue_name = "test_queue"
      queues = messenger.instance_variable_get(:@queues)
      queues[queue_name] = mock
      Messenger::Queue.expects(:new).never
      messenger.get_queue(queue_name)
    end

    it "should create the queue if it doesn't exist" do
      queue_name = 'test_queue'
      Messenger::Queue.expects(:new).with(queue_name,
                                        is_a(Messenger::Messenger),
                                        {})
      messenger.get_queue(queue_name)
    end

  end

  describe 'publish' do
    let (:queue_name) { "test_queue" }
    let (:message)    { "test message" }

    it 'should raise an error when attempting to publish to a nameless queue' do
      lambda {messenger.publish(message, '')}.should raise_error(ArgumentError)
    end

    context 'with good data' do

      it 'should create a queue and publish to it' do
        queue_name = 'test_queue'
        queue = mock('queue')
        queue.expects(:publish).with(message, Messenger::DEFAULT_PRIORITY, {})
        messenger.expects(:get_queue).with(queue_name).returns(queue)
        messenger.publish(message, queue_name)
      end

      it 'should pass custom headers to queue object' do
        test_headers = {:headers => {:operation => "TEST_OPERATION"}}
        queue = mock('queue')
        queue.expects(:publish).with(message, Messenger::DEFAULT_PRIORITY, test_headers)
        messenger.expects(:get_queue).with(queue_name).returns(queue)
        messenger.publish(message, queue_name, priority=Messenger::DEFAULT_PRIORITY, options=test_headers)
      end

    end
  end

  describe 'subscribe and unsubscribe' do
    it 'should raise an error if attempting to subscribe to a nameless queue' do
      lambda{messenger.subscribe('')}.should raise_error(ArgumentError)
    end

    it 'should raise an error if attempting to subscribe to a nil queue' do
      lambda{messenger.subscribe}.should raise_error(ArgumentError)
    end

    it 'should raise an error if attempting to unsubscribe from a nameless queue' do
      lambda{messenger.unsubscribe('')}.should raise_error(ArgumentError)
    end

    it 'should raise an error if attempting to unsubscribe from a nil queue' do
      lambda{messenger.unsubscribe}.should raise_error(ArgumentError)
    end

    context 'with good data' do
      let (:queue_name) { 'test_queue' }
      let (:queue)      { mock('queue') }

      it 'should should subscribe to the queue associated with the queue name provided' do
        queue.expects(:subscribed?).returns(false)
        queue.expects(:subscribe).with(is_a(Proc), is_a(Hash))
        messenger.expects(:get_queue).with(queue_name).returns(queue)
        messenger.subscribe(queue_name) { |m,p| puts 'ohai'}
      end

      it 'should unsubscribe from a subscribed queue' do
        queue.expects(:unsubscribe)
        messenger.expects(:get_queue).with(queue_name).returns(queue)
        messenger.unsubscribe(queue_name)
      end

      it 'should not attempt to unsubscribe from a queue that does not exist' do
        messenger.expects(:get_queue).with(queue_name).returns(nil)
        messenger.unsubscribe(queue_name)
      end

    end
  end

end
