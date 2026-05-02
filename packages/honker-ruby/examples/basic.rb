# frozen_string_literal: true
#
# Basic example: enqueue a few jobs, claim them, ack each.
#
#   ruby examples/basic.rb

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "honker"

EXT_PATH = ENV["HONKER_EXTENSION_PATH"] ||
           File.expand_path(
             "../../../target/release/libhonker_extension.dylib",
             __dir__,
           )

db = Honker::Database.new("demo.db", extension_path: EXT_PATH)
q  = db.queue("emails")

3.times do |i|
  q.enqueue({ to: "user-#{i}@example.com" })
end

loop do
  job = q.claim_one("worker-1")
  break if job.nil?

  puts "processing job #{job.id}: to=#{job.payload['to']}"
  job.ack
end

db.close
