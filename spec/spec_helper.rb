# This file is copied to spec/ when you run 'rails generate rspec:install'
require 'rubygems'
require 'spork'

require 'vb-lib'
require 'active_record'
require 'faker'
require 'mock_db'
require 'delayed_job'
#require 'delayed/backend/active_record' #TODO: should this be here?
require 'moqueue'
require 'rr'
require 'date'
require 'timecop'

overload_amqp

class MessageQueueMockUtils
  def self.mock_message(datum)
      msg = { 'instance' => [datum], 'meta' => {}}.to_json
  end

  def self.make_mock_message_queue_manager
    VB::MessageQueueManager.new('amqp://example.com', 'foobar', :client => MockAMQPClient.new, :exchange=>MockAMQPExchange.new('foobar'))
  end

  def self.make_mock_message_hub
    VB::Transporter::MessageHub.new(:queue_manager => MessageQueueMockUtils.make_mock_message_queue_manager)
  end
  def self.make_mock_priority_queue_poller(options={})
    exchange = MockAMQPExchange.new('foobar')
    high_amqp_queue = MockAMQPQueue.new(VB::Transporter::MessageHub::HIGH_QUEUE_NAME)
    low_amqp_queue =  MockAMQPQueue.new(VB::Transporter::MessageHub::LOW_QUEUE_NAME)
    fail_amqp_queue= MockAMQPQueue.new(VB::Transporter::MessageHub::FAIL_QUEUE_NAME)

    high_queue = VB::MessageQueue.new(high_amqp_queue, exchange)
    low_queue = VB::MessageQueue.new(low_amqp_queue, exchange)
    fail_queue = VB::MessageQueue.new(fail_amqp_queue, exchange)

    VB::Transporter::PriorityQueuePoller.new(high_queue, low_queue, fail_queue, options)
  end
end

class MockAMQPExchange
  def initialize(name)
    @@exchanges ||= Hash.new([])
    @@exchanges[name] = self
  end

  def publish(msg, options={})
    raise ArgumentError if options[:key].nil?
    queue = MockAMQPQueue.new(options[:key])
    queue.enqueue_msg(msg)
  end
end

class MockAMQPClient
  def exchange(*args)

  end

  def queue(name, *args)
    MockAMQPQueue.new(name)
  end
end

class MockAMQPQueue
  attr_reader :name

  @@queues = nil
  def all_queues
    @@queues
  end

  def self.clear_all_queues
    @@queues = {}
  end

  def initialize(name)
    @name = name
    @@queues = {} if @@queues.nil?
    @@queues[@name] = [] if @@queues[@name].nil?
  end

  def enqueue_msg(msg)
    @@queues[@name] << msg
  end

  def pop
    msg = @@queues[@name].slice!(0) || :queue_empty
    {:payload => msg}
  end

  def messages
    @@queues[@name]
  end

  def bind(*args)
    #do nothing
  end
end


class SpaceFighter < ActiveRecord::Base
  include VB::Transporter::TransportableMixin
  def all_has_associated_objects
    []
  end
  has_many   :space_carrier_fighters
  has_many   :space_carriers, :through => :space_carrier_fighters
  after_save do
    # A whitelist of columns that will be beamed out
    beam_out_options = self.respond_to?(:beam_out_columns) ?
    {:only => self.beam_out_columns} : {}
    self.beam_out(beam_out_options)
  end
end

class SpaceCarrierFighter < ActiveRecord::Base
  include VB::Transporter::TransportableMixin
  belongs_to :space_carrier
  belongs_to :space_fighter
end

class SpaceCarrier < ActiveRecord::Base
  include VB::Transporter::TransportableMixin
  belongs_to :space_fleet
  has_many   :space_carrier_fighters
  has_many   :space_fighters, :through => :space_carrier_fighters
end

class SpaceFleet < ActiveRecord::Base
  include VB::Transporter::TransportableMixin
  has_many   :space_carriers
end

# if spork is running, this runs once, before any test begins
Spork.prefork do
  #require File.expand_path("../../config/environment", __FILE__)
  #require 'rspec/rails'
  #require 'capybara/rspec'
  #require 'capybara/rails'
  #ActiveRecord::Base.connection_pool.disconnect!

  # Requires supporting ruby files with custom matchers and macros, etc,
  # in spec/support/ and its subdirectories.
end

# if spork is running, this runs every time rspec is called
Spork.each_run do
  # reload all spec support stuff
  #ActiveSupport::Dependencies.clear if Spork.using_spork?
  #Dir[Rails.root.join("spec/support/**/*.rb")].each {|f| require f}
end

RSpec.configure do |config|
  # == Mock Framework
  #
  # If you prefer to use mocha, flexmock or RR, uncomment the appropriate line:
  #
  # config.mock_with :mocha
  # config.mock_with :flexmock
  config.mock_with :rr
  #config.mock_with :rspec

  # Remove this line if you're not using ActiveRecord or ActiveRecord fixtures
  #config.fixture_path = "#{::Rails.root}/spec/fixtures"

  # If you're not using ActiveRecord, or you'd prefer not to run each of your
  # examples within a transaction, remove the following line or assign false
  # instead of true.
  #config.use_transactional_fixtures = true

  #config.extend VCR::RSpec::Macros
end

RSpec::Matchers.define :be_the_same_time_as do |expected_time|
  # Compares number of seconds, along with fraction, since epoch irrespective of time or datetime objects
  expected_secs  = expected_time.to_time.to_i
  expected_usecs = expected_time.to_time.usec
  match { |actual_time| (expected_secs == actual_time.to_time.to_i) && (expected_usecs == actual_time.to_time.usec) }
  failure_message_for_should do |actual_time|
    "Expected #{actual_time.to_time.to_i}.#{actual_time.to_time.usec} to be same as #{expected_secs}.#{expected_usecs}"
  end
  failure_message_for_should_not do |actual_time|
    "Expected #{actual_time.to_time.to_i}.#{actual_time.to_time.usec} to be different from #{expected_secs}.#{expected_usecs}"
  end
end

