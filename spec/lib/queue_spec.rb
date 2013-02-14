require 'messenger'
require 'pry'
require 'spec_helper'

describe Messenger::Queue do

  let (:channel) { mock('channel') }
  let (:messenger) do
    mock('messenger').tap do |m|
      m.stubs(:channel).returns(channel)
    end
  end
  let (:queue_name) { 'test_queue' }
  let (:queue) { Messenger::Queue.new(queue_name, messenger) }

  describe 'construction' do

    it 'should raise an exeception if a the queue name is nil' do
      lambda { Messenger::Queue.new(nil, messenger) }.should raise_exception(ArgumentError)
    end

    it 'should construct an AMQP queue for each priority level' do
      channel.expects(:queue).times(10)
      queue
    end

  end

  describe '' do
  end

end
