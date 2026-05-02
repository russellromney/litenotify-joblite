# frozen_string_literal: true
#
# Atomic business-write + enqueue in one transaction.
#
# The Ruby binding's Queue#enqueue goes through its own SQLite
# write; to make the job atomic with your INSERT, drive both from
# a single transaction on the underlying sqlite3 gem connection.
#
#   ruby -Ilib examples/atomic.rb

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "honker"
require "tmpdir"

EXT_PATH = ENV["HONKER_EXTENSION_PATH"] ||
           File.expand_path(
             "../../../target/release/libhonker_ext.dylib",
             __dir__,
           )

def count(raw, sql)
  raw.get_first_row(sql).first
end

Dir.mktmpdir("honker-") do |dir|
  db = Honker::Database.new(File.join(dir, "app.db"), extension_path: EXT_PATH)
  raw = db.db

  raw.execute(
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)"
  )

  # Success path.
  raw.transaction do
    raw.execute("INSERT INTO orders (user_id, total) VALUES (?, ?)", [42, 9900])
    raw.execute(
      "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
      [
        "emails",
        JSON.dump({ to: "alice@example.com", order_id: 42 }),
        nil, nil, 0, 3, nil,
      ],
    )
  end
  puts "committed: #{count(raw, 'SELECT COUNT(*) FROM orders')} order(s), " \
       "#{count(raw, "SELECT COUNT(*) FROM _honker_live WHERE queue='emails'")} job(s)"

  # Rollback path.
  begin
    raw.transaction do
      raw.execute("INSERT INTO orders (user_id, total) VALUES (?, ?)", [43, 5000])
      raw.execute(
        "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
        [
          "emails",
          JSON.dump({ to: "bob@example.com", order_id: 43 }),
          nil, nil, 0, 3, nil,
        ],
      )
      raise "boom — simulated payment-processing failure"
    end
  rescue StandardError => e
    puts "rolled back: #{e.message}"
  end

  puts "after rollback: #{count(raw, 'SELECT COUNT(*) FROM orders')} order(s), " \
       "#{count(raw, "SELECT COUNT(*) FROM _honker_live WHERE queue='emails'")} job(s)"
  puts "atomic enqueue + rollback both work as expected"

  db.close
end
