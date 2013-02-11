require 'messenger'
require 'pry'

RSpec.configure do |config|
  # == Mock Framework
  # config.mock_with :mocha
  config.mock_with :rr
  #config.mock_with :rspec
end

describe Messenger::Messenger do
  let (:amqp_url) { "amqp://localhost" }
  # A mocked up messenger object, with mocked AMQP methods
  let (:connection) { mock! }
  let (:channel) { mock! }
  let (:exchange) { mock! }
  let(:mock_thread) { mock! }
  let (:messenger) {
    mock(EventMachine).reactor_running?.times(any_times) { true }
    channel.default_exchange { exchange }
    mock(AMQP).connect(amqp_url, is_a(Hash)) { connection }
    mock(AMQP::Channel).new(is_a(Object), is_a(Hash)) { channel }
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

  describe 'queues' do

    it 'should raise an error if the queue name is blank' do
      lambda{ messenger.create_queue("") }.should raise_error(ArgumentError)
    end

    it 'should create the queue on the channel' do
      queue_name = "test_queue"
      channel.queue(queue_name, Messenger::Messenger::DEFAULT_QUEUE_OPTS)
      messenger.create_queue(queue_name)
    end

    it 'should not create the queue if it exists' do
      queue_name = "test_queue"
      mock_queue = mock!
      channel.queues { {queue_name => mock_queue} }
      messenger.create_queue_if_non_existent(queue_name)
    end

    it "should create the queue if it doesn't exist" do
      queue_name = "test_queue"
      mock_queue = mock!
      channel.queues {{}}
      channel.queue(queue_name, Messenger::Messenger::DEFAULT_QUEUE_OPTS)
      messenger.create_queue_if_non_existent(queue_name)
    end

  end

  describe 'publish' do
    let (:queue_name) { "test_queue" }
    let (:message) { "test message" }

    it 'should raise an error when attempting to publish to a nameless queue' do
      channel.queues {{}}
      lambda {messenger.publish(message, '')}.should raise_error(ArgumentError)
    end

    context 'with good data' do
      before(:each) do
        channel.queue(queue_name, Messenger::Messenger::DEFAULT_QUEUE_OPTS)
        channel.queues {{}}
      end
      it 'should publish a message to the exchange ' do
        exchange.publish(message, Messenger::Messenger::DEFAULT_MESSAGE_OPTS.
                         merge(:routing_key => queue_name))
        messenger.publish(message, queue_name)
      end

      it 'should handle custom headers correctly' do
        test_headers = {:headers => {:operation => "TEST_OPERATION"}}
        exchange.publish(message, Messenger::Messenger::DEFAULT_MESSAGE_OPTS.
                         merge(:routing_key => queue_name).
                         merge(test_headers))
        messenger.publish(message, queue_name, test_headers)
      end

    end

  end

  describe 'subscribe and unsubscribe' do
    it 'should raise an error if attempting to subscribe to a nameless queue' do
      lambda{messenger.subscribe('')}.should raise_error(ArgumentError)
    end

    it 'should raise an error if attempting to unsubscribe from a nameless queue' do
      lambda{messenger.unsubscribe('')}.should raise_error(ArgumentError)
    end

    context 'with good data' do
      let (:queue_name) { "test_queue" }
      let(:queue) { Object.new }

      it 'should should subscribe to the queue associated with the queue name provided' do
        mock(queue).subscribed? { false }
        mock(queue).subscribe(is_a(Hash))
        channel.queues.twice {{queue_name => queue}}
        messenger.subscribe(queue_name)
      end

      it 'should unsubscribe from a subscribed queue' do
        mock(queue).subscribed? { true }
        mock(queue).unsubscribe
        channel.queues.twice {{queue_name => queue}}
        messenger.unsubscribe(queue_name)
      end

      it 'should raise an error unsubscribing from a queue that has already been unsubscribed' do
        mock(queue).subscribed? { false }
        channel.queues.twice {{queue_name => queue}}
        lambda { messenger.unsubscribe(queue_name) }.should raise_error(Messenger::MessengerError)
      end

      describe 'message acking' do
        before(:each) do
          channel.queues.twice {{queue_name => queue}}
          mock(queue).subscribed? { false }
          mock(queue).subscribe(is_a(Hash)) { |*args| args[1] }
        end

        let(:metadata) { mock! }

        it 'should occur if no option is given' do
          mock(metadata).ack
          code = messenger.subscribe(queue_name)
          code[metadata, ""]
        end

        it 'should occur if ack is set to true' do
          mock(metadata).ack
          code = messenger.subscribe(queue_name, ack: true)
          code[metadata, ""]
        end

        it 'should not occur if ack is set to false in options' do
          dont_allow(metadata).ack
          code = messenger.subscribe(queue_name, ack: false)
          code[metadata, ""]
        end

      end

    end

  end

end
