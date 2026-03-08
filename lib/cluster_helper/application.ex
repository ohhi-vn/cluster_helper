defmodule ClusterHelper.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [ClusterHelper.NodeConfig]
    opts = [strategy: :one_for_one, name: ClusterHelper.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
