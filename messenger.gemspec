# -*- mode: ruby; encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'messenger/version'

Gem::Specification.new do |gem|
  gem.name          = "messenger"
  gem.version       = Messenger::VERSION
  gem.authors       = ["Rick Dillon"]
  gem.email         = ["rpdillon@apartmentlist.com"]
  gem.description   = %q{A wrapper around the AMQP gem to support easy messaging between applications.}
  gem.summary       = %q{Easy RabbitMQ Messaging}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_dependency('amqp')
  gem.add_dependency('rabbitmq-service-util')
  gem.add_dependency('algorithms')

  # TODO: add log_mixin here when it becomes a gem, delete from Gemfile
  # gem.add_dependency('log_mixin')

  gem.add_development_dependency('rspec')
  gem.add_development_dependency('pry')
  gem.add_development_dependency('rr')
  #gem.add_development_dependency('mocha')
end
