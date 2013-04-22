# -*- mode: ruby; encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'agni/version'

Gem::Specification.new do |gem|
  gem.name          = "agni"
  gem.version       = Agni::VERSION
  gem.authors       = ["Rick Dillon"]
  gem.email         = ["rpdillon@apartmentlist.com"]
  gem.description   = %q{A wrapper around the AMQP gem to support easy messaging between applications.}
  gem.summary       = %q{Easy RabbitMQ Messaging}
  gem.license       = '3-Clause BSD'
  gem.homepage      = "https://github.com/apartmentlist/agni"

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_dependency('amqp')
  gem.add_dependency('algorithms')
  gem.add_dependency('log_mixin')

  gem.add_development_dependency('rspec')
  gem.add_development_dependency('pry')
  gem.add_development_dependency('mocha')
end
