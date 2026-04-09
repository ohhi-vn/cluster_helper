defmodule ClusterHelper do
  @moduledoc """
  A helper module for managing dynamic Elixir clusters with **overlapping scope** support.

  `ClusterHelper` provides a simple, role-based API for tracking nodes in a
  distributed cluster. It is designed for dynamic environments such as Kubernetes.

  Note: Collab between human & AI.

  ## Features

  - **Role-based node management** – assign one or more roles to each node;
    roles can be any Elixir term (atom, string, tuple, …)
  - **Overlapping scopes** – a node can participate in multiple scopes
    simultaneously with full isolation between them. Different roles can be
    assigned to different scopes.
  - **Dynamic cluster support** – nodes are discovered and tracked automatically
    as they join or leave the cluster via `:net_kernel.monitor_nodes/2`
  - **Microsecond lookups** – read operations (`get_nodes/2`, `get_roles/2`,
    `all_nodes/1`) bypass the GenServer and read directly from ETS tables
    with `read_concurrency: true`
  - **Generation-based sync** – each scope tracks a monotonically increasing
    generation counter; periodic pulls only do a full RPC when the remote
    node's generation has changed, dramatically reducing unnecessary traffic
  - **Smart node discovery** – on `:nodeup`, the remote node's scope membership
    is queried first so roles are only pulled for matching scopes
  - **Cluster-wide synchronisation** – changes propagate via `:pg` broadcast
    and a configurable periodic pull with generation-based change detection
  - **Event callbacks** – optional `ClusterHelper.EventHandler` behaviour
    notifies your code when roles or nodes are added **or removed**

  ## Quick start

      # Tag the current node (default scope)
      ClusterHelper.add_role(:web)
      ClusterHelper.add_roles([:api, "cache", {:shard, 1}])

      # Tag the current node in a specific scope
      ClusterHelper.add_role(:worker, :data_processing)
      ClusterHelper.add_roles([:ingester, :aggregator], :analytics)

      # Join an additional scope at runtime
      ClusterHelper.join_scope(:monitoring)
      ClusterHelper.add_role(:prometheus_exporter, :monitoring)

      # Query the cluster (default scope)
      ClusterHelper.get_nodes(:web)              #=> [:"node1@host", :"node2@host"]
      ClusterHelper.get_roles(:"node1@host")     #=> [:web, :api]
      ClusterHelper.all_nodes()                  #=> [:"node1@host", :"node2@host"]

      # Query a specific scope
      ClusterHelper.get_nodes(:worker, :data_processing)
      #=> [:"node1@host"]

      ClusterHelper.get_roles(:"node1@host", :analytics)
      #=> [:ingester, :aggregator]

      # Check or remove roles
      ClusterHelper.get_my_roles()               #=> [:web, :api, "cache", {:shard, 1}]
      ClusterHelper.remove_role("cache")
      ClusterHelper.remove_roles([{:shard, 1}])

      # List all scopes the local node participates in
      ClusterHelper.list_scopes()
      #=> [:cluster_helper, :data_processing, :analytics, :monitoring]

      # Leave a scope
      ClusterHelper.leave_scope(:monitoring)

  ## Configuration (`config/config.exs`)

      config :cluster_helper,
        # Roles applied at startup in the default scope.
        # Any Elixir term is a valid role.
        roles: [:data, :web],
        # Scopes to join at startup (enables overlapping scopes).
        # If omitted, defaults to a single scope from :scope or :cluster_helper.
        scopes: [:default, :data_processing, :analytics],
        # Default scope name (used when no scope is specified in API calls).
        # Default: ClusterHelper
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

  ### Generation-based sync

  Each scope maintains a local `generation` counter (starting at 0) that is
  incremented every time roles are added or removed on this node. During the
  periodic pull, the GenServer first checks the remote node's generation via a
  lightweight `:erpc.call`. Only if the generation has changed (or the node is
  newly discovered) does a full role pull occur. This avoids expensive full-sync
  RPCs when nothing has changed, significantly reducing cluster traffic.

  ### Smart node up/down handling

  When a new node connects (`:nodeup`), `ClusterHelper` queries the remote
  node's scope membership via `__get_scopes__/0` and only pulls roles for
  scopes that both nodes share. This avoids unnecessary cross-scope RPCs.

  When a node disconnects (`:nodedown`), all its roles are removed from every
  scope and the `on_node_removed` callback is fired.

  ### Scope isolation

  Each scope maintains its own:
  - Role-to-node mappings
  - Node-to-role mappings
  - `:pg` process group for messaging
  - Known-nodes tracking
  - Generation counter for efficient sync

  A node can have `:web` role in scope `:frontend` and `:worker` role in scope
  `:backend` simultaneously. Queries are always scoped, so there is no cross-
  scope leakage.

  ### Backward compatibility

  All API functions accept an optional `scope` parameter that defaults to `nil`.
  When `nil`, the configured default scope is used. Existing code that does not
  pass a scope will continue to work unchanged.
  """

  alias ClusterHelper.NodeConfig

  # Roles can be any Elixir term.
  @type role :: term()
  @type node_name :: node()
  @type scope :: atom()

  # ── Role queries ─────────────────────────────────────────────────────────────

  @doc """
  Returns every node in the given `scope` that has been assigned `role`.

  If `scope` is `nil`, uses the default scope. Returns `[]` when no node
  carries that role in the specified scope.

  ## Examples

      ClusterHelper.add_role(:web)
      ClusterHelper.get_nodes(:web)
      #=> [:"node1@127.0.0.1"]

      ClusterHelper.add_role(:worker, :data_processing)
      ClusterHelper.get_nodes(:worker, :data_processing)
      #=> [:"node1@127.0.0.1"]

      ClusterHelper.get_nodes({:shard, 1})
      #=> []
  """
  @spec get_nodes(role(), scope :: scope() | nil) :: [node_name()]
  def get_nodes(role, scope \\ nil), do: NodeConfig.get_nodes(role, scope)

  @doc """
  Returns all roles assigned to `node` in the given `scope`.

  If `scope` is `nil`, uses the default scope. Returns `[]` when the node
  is unknown or has no roles in that scope.

  ## Examples

      ClusterHelper.add_roles([:web, {:shard, 2}])
      ClusterHelper.get_roles(Node.self())
      #=> [:web, {:shard, 2}]

      ClusterHelper.get_roles(:"node1@127.0.0.1", :analytics)
      #=> [:ingester, :aggregator]
  """
  @spec get_roles(node_name(), scope :: scope() | nil) :: [role()]
  def get_roles(node, scope \\ nil), do: NodeConfig.get_roles(node, scope)

  @doc """
  Returns a deduplicated list of every node that has at least one role in the
  given `scope`.

  If `scope` is `nil`, uses the default scope.

  ## Examples

      ClusterHelper.all_nodes()
      #=> [:"node1@127.0.0.1", :"node2@127.0.0.1"]

      ClusterHelper.all_nodes(:data_processing)
      #=> [:"node1@127.0.0.1"]
  """
  @spec all_nodes(scope :: scope() | nil) :: [node_name()]
  def all_nodes(scope \\ nil), do: NodeConfig.all_nodes(scope)

  # ── Local-node role management ────────────────────────────────────────────────

  @doc """
  Returns all roles currently assigned to this node in the given `scope`.

  If `scope` is `nil`, uses the default scope.

  ## Examples

      ClusterHelper.add_roles([:web, "api"])
      ClusterHelper.get_my_roles()
      #=> [:web, "api"]

      ClusterHelper.get_my_roles(:analytics)
      #=> [:ingester]
  """
  @spec get_my_roles(scope :: scope() | nil) :: [role()]
  def get_my_roles(scope \\ nil), do: NodeConfig.get_my_roles(scope)

  @doc """
  Adds `role` to the current node in the given `scope` and propagates the change
  cluster-wide.

  If `scope` is `nil`, uses the default scope. The scope is automatically
  joined if it hasn't been already.

  `role` can be any Elixir term. Duplicate roles are silently ignored.

  ## Examples

      ClusterHelper.add_role(:web)
      ClusterHelper.add_role({:shard, 1})
      ClusterHelper.get_my_roles()
      #=> [:web, {:shard, 1}]

      ClusterHelper.add_role(:worker, :data_processing)
      ClusterHelper.get_my_roles(:data_processing)
      #=> [:worker]
  """
  @spec add_role(role(), scope :: scope() | nil) :: :ok
  def add_role(role, scope \\ nil), do: NodeConfig.add_role(role, scope)

  @doc """
  Adds each role in `roles` to the current node in the given `scope` and
  propagates cluster-wide.

  If `scope` is `nil`, uses the default scope. All new roles are announced in
  a single pub/sub event. Duplicates are filtered. Roles can be any Elixir term.

  ## Examples

      ClusterHelper.add_roles([:web, "api", {:shard, 1}])
      ClusterHelper.get_my_roles()
      #=> [:web, "api", {:shard, 1}]

      ClusterHelper.add_roles([:ingester, :aggregator], :analytics)
      ClusterHelper.get_my_roles(:analytics)
      #=> [:ingester, :aggregator]
  """
  @spec add_roles([role()], scope :: scope() | nil) :: :ok
  def add_roles(roles, scope \\ nil), do: NodeConfig.add_roles(roles, scope)

  @doc """
  Removes `role` from the current node in the given `scope` and propagates the
  change cluster-wide.

  If `scope` is `nil`, uses the default scope.

  ## Examples

      ClusterHelper.add_roles([:web, "api"])
      ClusterHelper.remove_role("api")
      ClusterHelper.get_my_roles()
      #=> [:web]

      ClusterHelper.remove_role(:worker, :data_processing)
  """
  @spec remove_role(role(), scope :: scope() | nil) :: :ok
  def remove_role(role, scope \\ nil), do: NodeConfig.remove_role(role, scope)

  @doc """
  Removes each role in `roles` from the current node in the given `scope` and
  propagates cluster-wide.

  If `scope` is `nil`, uses the default scope.

  ## Examples

      ClusterHelper.add_roles([:web, "api", {:shard, 1}])
      ClusterHelper.remove_roles(["api", {:shard, 1}])
      ClusterHelper.get_my_roles()
      #=> [:web]

      ClusterHelper.remove_roles([:ingester, :aggregator], :analytics)
  """
  @spec remove_roles([role()], scope :: scope() | nil) :: :ok
  def remove_roles(roles, scope \\ nil), do: NodeConfig.remove_roles(roles, scope)

  # ── Scope management ──────────────────────────────────────────────────────────

  @doc """
  Joins the current node to an additional `scope`.

  The scope is started (if not already), the node joins the `:all_nodes` group,
  and an initial pull is performed from all connected nodes for this scope.

  Returns `:ok` on success, or `{:error, :already_joined}` if the node is
  already a member of the scope.

  ## Examples

      ClusterHelper.join_scope(:monitoring)
      #=> :ok

      ClusterHelper.join_scope(:monitoring)
      #=> {:error, :already_joined}
  """
  @spec join_scope(scope()) :: :ok | {:error, :already_joined}
  def join_scope(scope), do: NodeConfig.join_scope(scope)

  @doc """
  Leaves the given `scope`, removing all local roles and cleaning up ETS entries
  for this scope.

  Returns `:ok` on success, or `{:error, :not_joined}` if the node is not
  a member of the scope.

  ## Examples

      ClusterHelper.leave_scope(:monitoring)
      #=> :ok

      ClusterHelper.leave_scope(:monitoring)
      #=> {:error, :not_joined}
  """
  @spec leave_scope(scope()) :: :ok | {:error, :not_joined}
  def leave_scope(scope), do: NodeConfig.leave_scope(scope)

  @doc """
  Returns a list of all scopes the local node is currently participating in.

  ## Examples

      ClusterHelper.add_role(:web)
      ClusterHelper.join_scope(:analytics)
      ClusterHelper.list_scopes()
      #=> [:cluster_helper, :analytics]
  """
  @spec list_scopes() :: [scope()]
  def list_scopes(), do: NodeConfig.list_scopes()

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
