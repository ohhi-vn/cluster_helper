defmodule ClusterHelper.MixProject do
  use Mix.Project

  def project do
    [
      app: :cluster_helper,
      version: "0.6.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Docs
      name: "ClusterHelper",
      source_url: "https://github.com/ohhi-vn/cluster_helper",
      homepage_url: "https://ohhi.vn",
      docs: docs(),
      description: description(),
      package: package(),
      aliases: aliases(),
      usage_rules: usage_rules()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {ClusterHelper.Application, []}
    ]
  end

  # Mix 1.19+ requires CLI preferences to live here, not in project/0.
  def cli do
    [
      preferred_envs: [
        "test.cluster": :test,
        "test.multi_node_scale": :test,
        "test.all": :test
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:benchee, "~> 1.5", only: :dev},
      {:tidewave, "~> 0.5", only: :dev},
      {:bandit, "~> 1.10", only: :dev},
      {:usage_rules, "~> 1.2", only: [:dev]},

      # Test dependencies
      {:excoveralls, "~> 0.18", only: :test},

      # Code quality
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false}
    ]
  end

  defp description() do
    "A tiny library for lookup node(s) in dynamic clustering. Map node to roles or id and allows you to easily select node(s) by role/id."
  end

  defp package() do
    [
      maintainers: ["Manh Vu"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/ohhi-vn/cluster_helper",
        "About us" => "https://ohhi.vn/"
      }
    ]
  end

  defp docs do
    [
      main: "README",
      extras: extras()
    ]
  end

  defp extras do
    list =
      "guides/**/*.md"
      |> Path.wildcard()

    list = list ++ ["README.md"]

    list
    |> Enum.map(fn path ->
      title =
        path
        |> Path.basename(".md")
        |> String.split(~r|[-_]|)
        |> Enum.map_join(" ", &String.capitalize/1)
        |> case do
          "F A Q" -> "FAQ"
          no_change -> no_change
        end

      {String.to_atom(path),
       [
         title: title,
         default: title == "Guide"
       ]}
    end)
  end

  defp aliases do
    [
      tidewave:
        "run --no-halt -e 'Agent.start(fn -> Bandit.start_link(plug: Tidewave, port: 4112) end)'",
      "usage_rules.update": [
        """
        usage_rules.sync AGENTS.md --all \
          --inline usage_rules:all \
          --link-to-folder deps
        """
        |> String.trim()
      ],

      # Default `mix test` – skip the real-cluster tests for fast feedback.
      test: ["test --exclude cluster"],

      # Real-cluster tests only (2-3 nodes).
      # `--name` is a VM flag so it must be given to the `elixir` binary, not to
      # `mix test`. The `cmd` task runs a shell command, letting us prefix with
      # `elixir --name ... -S mix`.
      "test.cluster": "cmd elixir --name test@127.0.0.1 -S mix test --only cluster",

      # Multi-node scale tests (10-20+ nodes with churn simulation).
      "test.multi_node_scale":
        "cmd elixir --name test@127.0.0.1 -S mix test --only multi_node_scale",

      # Full suite – unit + cluster + multi-node scale.
      "test.all":
        "cmd elixir --name test@127.0.0.1 -S mix test --include cluster --include multi_node_scale",
      # Testing & Coverage
      coveralls: ["test --cover", "coveralls.html"],
      # Code Quality
      quality: ["format --check-formatted", "credo --strict", "dialyzer"]
    ]
  end

  defp usage_rules do
    [
      file: "AGENTS.md",
      usage_rules: :all
    ]
  end
end
