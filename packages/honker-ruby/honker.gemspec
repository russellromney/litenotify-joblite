# frozen_string_literal: true

require_relative "lib/honker/version"

Gem::Specification.new do |spec|
  spec.name          = "honker"
  spec.version       = Honker::VERSION
  spec.authors       = ["Russell Romney"]

  spec.summary       = "Durable queues, streams, pub/sub, and scheduler on SQLite."
  spec.description   = <<~DESC.strip
    Ruby binding for Honker — a SQLite-native task runtime. Queues,
    streams, pub/sub, time-trigger scheduler, results, locks, rate limits, all
    in one .db file. Thin wrapper around the Honker SQLite loadable
    extension; no Redis, no external broker.
  DESC
  spec.homepage      = "https://honker.dev"
  spec.license       = "Apache-2.0"
  spec.required_ruby_version = ">= 3.0.0"

  spec.metadata["homepage_uri"]    = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/russellromney/honker"
  spec.metadata["documentation_uri"] = "https://honker.dev/"

  spec.files = Dir.glob("lib/**/*") + %w[honker.gemspec README.md]
  spec.require_paths = ["lib"]

  spec.add_dependency "sqlite3", ">= 1.7"
end
