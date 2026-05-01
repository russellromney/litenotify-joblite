defmodule Honker.MixProject do
  use Mix.Project

  def project do
    [
      app: :honker,
      version: "0.1.1",
      elixir: "~> 1.17",
      description:
        "Durable queues, streams, pub/sub, and scheduler on SQLite. " <>
          "Thin wrapper around the Honker loadable extension.",
      package: package(),
      deps: deps(),
      # mix.exs lives in its own directory (not part of a parent umbrella);
      # keep mix's build artifacts local so a submodule split is clean.
      build_path: "_build",
      deps_path: "deps"
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp deps do
    [
      {:exqlite, "~> 0.24"},
      {:jason, "~> 1.4"}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/russellromney/honker",
        "Docs" => "https://honker.dev"
      }
    ]
  end
end
