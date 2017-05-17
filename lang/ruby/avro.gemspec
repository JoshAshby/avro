# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'avro/version'

Gem::Specification.new do |spec|
  spec.name          = "avro"
  spec.version       = Avro::VERSION

  spec.authors       = ["Apache Software Foundation"]
  spec.email         = ["dev@avro.apache.org"]

  spec.summary       = "Apache Avro for Ruby"
  spec.description   = "Avro is a data serialization and RPC format"

  spec.homepage      = "http://avro.apache.org/"
  spec.licenses      = ["Apache License 2.0 (Apache-2.0)"]

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.rdoc_options = ["--line-numbers", "--title", "Avro"]

  spec.add_development_dependency "bundler", "~> 1.11"
  spec.add_development_dependency "rake", ">= 12.0"

  spec.add_runtime_dependency "multi_json", "~> 1.12"
  spec.add_runtime_dependency "snappy", "~> 0.0.15"
end
