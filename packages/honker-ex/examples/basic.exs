# Basic example: enqueue a few jobs, claim them, ack each.
#
#   mix run examples/basic.exs

ext_path =
  System.get_env("HONKER_EXTENSION_PATH") ||
    Path.expand("../../../target/release/libhonker_ext.dylib", __DIR__)

{:ok, db} = Honker.open("demo.db", extension_path: ext_path)

for i <- 0..2 do
  Honker.Queue.enqueue(db, "emails", %{"to" => "user-#{i}@example.com"})
end

drain = fn drain ->
  case Honker.Queue.claim_one(db, "emails", "worker-1") do
    {:ok, nil} ->
      :done

    {:ok, job} ->
      IO.puts("processing job #{job.id}: to=#{job.payload["to"]}")
      Honker.Job.ack(db, job)
      drain.(drain)
  end
end

drain.(drain)
