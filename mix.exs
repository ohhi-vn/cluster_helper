defmodule ClusterHelper.MixProject do
  use Mix.Project

  def project do
    [
      app: :cluster_helper,
      version: "0.1.0",
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
      aliases: aliases()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :syn],
      mod: {ClusterHelper.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:syn, "~> 3.3"},
      {:ex_doc, "~> 0.38", only: :dev, runtime: false},
      {:benchee, "~> 1.4", only: :dev},
      {:tidewave, "~> 0.5", only: :dev},
      {:bandit, "~> 1.8", only: :dev},
      {:usage_rules, "~> 0.1", only: [:dev]}
    ]
  end

  defp description() do
    "A library for dynamic cluster like Kubernetes. Easy to get nodes by role for calling RPC."
  end

  defp package() do
    [
      maintainers: ["Manh Van Vu"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/ohhi-vn/easy_rpc",
        "About us" => "https://ohhi.vn/"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
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
      ]
    ]
  end
end
