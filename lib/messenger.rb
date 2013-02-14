require 'log_mixin'
require 'amqp'
require 'algorithms'
require 'messenger/version'
require 'messenger/queue'
require 'messenger/messenger'
require 'messenger/messenger_error'

module Messenger
  # Enforce durability-by-default at the queue and message level
  DEFAULT_QUEUE_OPTS = {durable: true}.freeze
  DEFAULT_MESSAGE_OPTS = {persistent: true}.freeze
  DEFAULT_CHANNEL_OPTS = {
    auto_recovery: true,
  }.freeze
  DEFAULT_CONNECTION_OPTS = {
    auto_recovery: true,
    # Log cases in which we're unexpectedly disconnected from the server
    on_tcp_connection_failure: ->{ warning("TCP connection failure detected") }
  }
  # Each logical queue has 10 AMQP queues backing it to support
  # prioritization.  Those priorities are numbered 0 (highest)
  # through 9 (lowest).
  PRIORITY_LEVELS = (0..9).to_a.freeze
  # To allow room above and below the default, we put it in the middle
  DEFAULT_PRIORITY = 4
  DEFAULT_PREFETCH = 50
end
