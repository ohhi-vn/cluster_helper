defmodule ClusterHelper do
  @moduledoc """
  A helper module for managing dynamic Elixir clusters.

  `ClusterHelper` provides a simple, role-based API for tracking nodes in a
  distributed cluster. It is built on top of the `:syn` library and is designed
  for dynamic environments such as Kubernetes.

  Note: Collab between human & AI.

  ## Features

  - **Role-based node management** – assign one or more roles to each node;
    roles can be any Elixir term (atom, string, tuple, …)
  - **Dynamic cluster support** – nodes are discovered and tracked automatically
    as they join or leave the cluster
  - **Fast lookups** – ETS-backed queries for both role→nodes and node→roles
  - **Cluster-wide synchronisation** – changes propagate via pub/sub and a
    configurable periodic pull
  - **Event callbacks** – optional `ClusterHelper.EventHandler` behaviour
    notifies your code when roles or nodes are added

  ## Quick start

      # Tag the current node
      ClusterHelper.add_role(:web)
      ClusterHelper.add_roles([:api, "cache", {:shard, 1}])

      # Query the cluster
      ClusterHelper.get_nodes(:web)              #=> [:"node1@host", :"node2@host"]
      ClusterHelper.get_roles(:"node1@host")     #=> [:web, :api]
      ClusterHelper.all_nodes()                  #=> [:"node1@host", :"node2@host"]

      # Check or remove roles
      ClusterHelper.get_my_roles()               #=> [:web, :api, "cache", {:shard, 1}]
      ClusterHelper.remove_role("cache")
      ClusterHelper.remove_roles([{:shard, 1}])

  ## Configuration (`config/config.exs`)

      config :cluster_helper,
        # Roles applied at startup (can also be changed at runtime).
        # Any Elixir term is a valid role.
        roles: [:data, :web],
        # Scope for the :syn library (default: ClusterHelper)
        scope: :my_cluster,
        # Timeout for remote role-pull RPC calls (default: 5_000 ms)
        pull_timeout: 5_000,
        # Interval between periodic background syncs (default: 7_000 ms)
        pull_interval: 10_000,
        # Optional event-handler module implementing ClusterHelper.EventHandler
        event_handler: MyApp.ClusterEvents

  ## Notes

  Nodes must first be connected at the Erlang distribution level (e.g. via
  [libcluster](https://hex.pm/packages/libcluster) or `Node.connect/1`).
  `ClusterHelper` only tracks *roles* – it does not handle node discovery.
  """

  alias ClusterHelper.NodeConfig

  # Roles can be any Elixir term.
  @type role :: term()
  @type node_name :: node()

  # ── Role queries ─────────────────────────────────────────────────────────────

  @doc """
  Returns every node in the cluster that has been assigned `role`.

  `role` can be any Elixir term. Returns `[]` when no node carries that role.

  ## Examples

      ClusterHelper.add_role(:web)
      ClusterHelper.get_nodes(:web)
      #=> [:"node1@127.0.0.1"]

      ClusterHelper.get_nodes({:shard, 1})
      #=> []
  """
  @spec get_nodes(role()) :: [node_name()]
  def get_nodes(role), do: NodeConfig.get_nodes(role)

  @doc """
  Returns all roles assigned to `node`.

  Returns `[]` when the node is unknown or has no roles.

  ## Examples

      ClusterHelper.add_roles([:web, {:shard, 2}])
      ClusterHelper.get_roles(Node.self())
      #=> [:web, {:shard, 2}]
  """
  @spec get_roles(node_name()) :: [role()]
  def get_roles(node), do: NodeConfig.get_roles(node)

  @doc """
  Returns a deduplicated list of every node that has at least one role.

  ## Examples

      ClusterHelper.all_nodes()
      #=> [:"node1@127.0.0.1", :"node2@127.0.0.1"]
  """
  @spec all_nodes() :: [node_name()]
  def all_nodes(), do: NodeConfig.get_all_nodes()

  # ── Local-node role management ────────────────────────────────────────────────

  @doc """
  Returns all roles currently assigned to this node.

  ## Examples

      ClusterHelper.add_roles([:web, "api"])
      ClusterHelper.get_my_roles()
      #=> [:web, "api"]
  """
  @spec get_my_roles() :: [role()]
  def get_my_roles(), do: NodeConfig.get_my_roles()

  @doc """
  Adds `role` to the current node and propagates the change cluster-wide.

  `role` can be any Elixir term. Duplicate roles are silently ignored.

  ## Examples

      ClusterHelper.add_role(:web)
      ClusterHelper.add_role({:shard, 1})
      ClusterHelper.get_my_roles()
      #=> [:web, {:shard, 1}]
  """
  @spec add_role(role()) :: :ok
  def add_role(role), do: NodeConfig.add_role(role)

  @doc """
  Adds each role in `roles` to the current node and propagates cluster-wide.

  All new roles are announced in a single pub/sub event. Duplicates are filtered.
  Roles can be any Elixir term.

  ## Examples

      ClusterHelper.add_roles([:web, "api", {:shard, 1}])
      ClusterHelper.get_my_roles()
      #=> [:web, "api", {:shard, 1}]
  """
  @spec add_roles([role()]) :: :ok
  def add_roles(roles), do: NodeConfig.add_roles(roles)

  @doc """
  Removes `role` from the current node and propagates the change cluster-wide.

  ## Examples

      ClusterHelper.add_roles([:web, "api"])
      ClusterHelper.remove_role("api")
      ClusterHelper.get_my_roles()
      #=> [:web]
  """
  @spec remove_role(role()) :: :ok
  def remove_role(role), do: NodeConfig.remove_role(role)

  @doc """
  Removes each role in `roles` from the current node and propagates cluster-wide.

  ## Examples

      ClusterHelper.add_roles([:web, "api", {:shard, 1}])
      ClusterHelper.remove_roles(["api", {:shard, 1}])
      ClusterHelper.get_my_roles()
      #=> [:web]
  """
  @spec remove_roles([role()]) :: :ok
  def remove_roles(roles), do: NodeConfig.remove_roles(roles)

  # ── Utility ───────────────────────────────────────────────────────────────────

  @doc """
  Returns `true` when `node` is the local node, `false` otherwise.

  ## Examples

      ClusterHelper.local_node?(Node.self())
      #=> true

      ClusterHelper.local_node?(:"other@127.0.0.1")
      #=> false
  """
  @spec local_node?(node_name()) :: boolean()
  def local_node?(node), do: NodeConfig.local_node?(node)
end
