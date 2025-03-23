defmodule ClusterHelper.MixProject do
  use Mix.Project

  def project do
    [
      app: :cluster_helper,
      version: "0.0.6",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # Docs
      name: "ClusterHelper",
      source_url: "https://github.com/ohhi-vn/cluster_helper",
      homepage_url: "https://ohhi.vn",
      docs: docs(),
      description: description(),
      package: package()
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
      {:ex_doc, "~> 0.24", only: :dev, runtime: false},
      {:benchee, "~> 1.3", only: :dev},
    ]
  end


  defp description() do
    "A library for dynamic cluster like Kubernetes. Easy to get nodes by role for calling RPC."
  end

  defp package() do
    [
      maintainers: ["Manh Van Vu"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/ohhi-vn/easy_rpc", "About us" => "https://ohhi.vn/"}
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
          "F A Q" ->"FAQ"
          no_change -> no_change
        end

      {String.to_atom(path),
        [
          title: title,
          default: title == "Guide"
        ]
      }
    end)
  end
end
