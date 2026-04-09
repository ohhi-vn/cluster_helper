defmodule ClusterHelper.EventHandler do
  @moduledoc """
  Optional callback behaviour for `ClusterHelper` events.

  Implement this behaviour in any module and set it in your config:

      config :cluster_helper, event_handler: MyApp.ClusterEvents

  All callbacks are optional — implement only the ones you need.

  ## Example
      ```elixir
      defmodule MyApp.ClusterEvents do
        @behaviour ClusterHelper.EventHandler

        @impl true
        def on_role_added(node, role) do
          Logger.info("Node \#{node} gained role \#{inspect(role)}")
        end

        @impl true
        def on_role_removed(node, role) do
          Logger.info("Node \#{node} lost role \#{inspect(role)}")
        end

        @impl true
        def on_node_added(node) do
          Logger.info("New node joined the cluster: \#{node}")
        end

        @impl true
        def on_node_removed(node) do
          Logger.info("Node left the cluster: \#{node}")
        end
      end
      ```
  """

  @doc """
  Called whenever a role is inserted into the local ETS table for any node
  (local or remote). Fired once per role, even when a batch is added.

  `node` is the node that owns the role; `role` can be any Elixir term.
  """
  @callback on_role_added(node :: node(), role :: ClusterHelper.role()) :: :ok

  @doc """
  Called when a previously unknown node is discovered for the first time
  (i.e. on the initial pull after it joins the cluster).
  """
  @callback on_node_added(node :: node()) :: :ok

  @doc """
  Called whenever a role is removed from a node in the local ETS table.
  Fired once per role, even when a batch is removed.

  `node` is the node that lost the role; `role` can be any Elixir term.
  """
  @callback on_role_removed(node :: node(), role :: ClusterHelper.role()) :: :ok

  @doc """
  Called when a node goes down and is removed from the cluster.
  """
  @callback on_node_removed(node :: node()) :: :ok

  @optional_callbacks on_role_added: 2, on_node_added: 1, on_role_removed: 2, on_node_removed: 1

  @doc false
  @spec dispatch_role_added(node(), ClusterHelper.role()) :: :ok
  def dispatch_role_added(node, role) do
    with {:ok, mod} <- configured_handler(),
         true <- function_exported?(mod, :on_role_added, 2) do
      mod.on_role_added(node, role)
    end

    :ok
  end

  @doc false
  @spec dispatch_node_added(node()) :: :ok
  def dispatch_node_added(node) do
    with {:ok, mod} <- configured_handler(),
         true <- function_exported?(mod, :on_node_added, 1) do
      mod.on_node_added(node)
    end

    :ok
  end

  @doc false
  @spec dispatch_role_removed(node(), ClusterHelper.role()) :: :ok
  def dispatch_role_removed(node, role) do
    with {:ok, mod} <- configured_handler(),
         true <- function_exported?(mod, :on_role_removed, 2) do
      mod.on_role_removed(node, role)
    end

    :ok
  end

  @doc false
  @spec dispatch_node_removed(node()) :: :ok
  def dispatch_node_removed(node) do
    with {:ok, mod} <- configured_handler(),
         true <- function_exported?(mod, :on_node_removed, 1) do
      mod.on_node_removed(node)
    end

    :ok
  end

  defp configured_handler do
    case Application.get_env(:cluster_helper, :event_handler) do
      nil -> :no_handler
      mod -> {:ok, mod}
    end
  end
end
