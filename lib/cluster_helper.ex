defmodule ClusterHelper do
  @moduledoc """
  A helper module for managing dynamic Elixir clusters.

  `ClusterHelper` provides a simple API for managing node roles in a distributed
  cluster environment. It's built on top of the `:syn` library and is designed
  to work seamlessly in dynamic cluster environments like Kubernetes.

  ## Features

  - **Role-based node management**: Assign one or multiple roles to nodes
  - **Dynamic cluster support**: Automatically discovers and tracks nodes as they join/leave
  - **Fast lookups**: Uses ETS for efficient role-to-node and node-to-role queries
  - **Cluster-wide synchronization**: Changes are automatically propagated across the cluster

  ## Usage

  Add roles to the current node:

      ClusterHelper.add_role(:web)
      ClusterHelper.add_roles([:api, :cache])

  Query nodes by role:

      ClusterHelper.get_nodes(:web)
      #=> [:node1@host, :node2@host]

  Get all roles for a specific node:

      ClusterHelper.get_roles(:node1@host)
      #=> [:web, :api]

  Get all nodes in the cluster:

      ClusterHelper.all_nodes()
      #=> [:node1@host, :node2@host, :node3@host]

  ## Configuration

  Configure in `config/config.exs`:

      config :cluster_helper,
        # Optional: Initial roles for this node
        roles: [:data, :web],
        # Optional: Scope for :syn library (default: ClusterHelper)
        scope: :my_cluster,
        # Optional: Timeout for sync between nodes (default: 5_000ms)
        pull_timeout: 5_000,
        # Optional: Interval for pulling from other nodes (default: 7_000ms)
        pull_interval: 10_000

  """

  alias ClusterHelper.NodeConfig

  @type role :: atom()
  @type node_name :: node()

  @doc """
  Returns all nodes in the cluster that have the specified role.

  ## Parameters

    * `role` - The role to query for (atom)

  ## Returns

  A list of nodes that have the specified role. Returns an empty list if
  no nodes have the role.

  ## Examples

      # Add a role first, then query
      ClusterHelper.add_role(:web)
      ClusterHelper.get_nodes(:web)
      # => [:"node1@127.0.0.1"]

      ClusterHelper.get_nodes(:nonexistent_role)
      # => []

  """
  @spec get_nodes(role()) :: [node_name()]
  def get_nodes(role) do
    NodeConfig.get_nodes(role)
  end

  @doc """
  Returns all roles assigned to the specified node.

  ## Parameters

    * `node` - The node to query for (node name)

  ## Returns

  A list of roles assigned to the node. Returns an empty list if the node
  has no roles or doesn't exist in the cluster.

  ## Examples

      # Add roles first, then query
      ClusterHelper.add_roles([:web, :api])
      ClusterHelper.get_roles(Node.self())
      # => [:web, :api]

  """
  @spec get_roles(node_name()) :: [role()]
  def get_roles(node) do
    NodeConfig.get_roles(node)
  end

  @doc """
  Returns all nodes currently in the cluster.

  This returns a deduplicated list of all nodes that have at least one role.

  ## Returns

  A list of all nodes in the cluster.

  ## Examples

      ClusterHelper.all_nodes()
      # => [:"node1@127.0.0.1", :"node2@127.0.0.1"]

  """
  @spec all_nodes() :: [node_name()]
  def all_nodes do
    NodeConfig.get_all_nodes()
  end

  @doc """
  Adds a single role to the current node.

  The role is immediately added locally and propagated to all other nodes
  in the cluster via pub/sub.

  ## Parameters

    * `role` - The role to add (atom)

  ## Returns

  `:ok`

  ## Examples

      ClusterHelper.add_role(:web)
      ClusterHelper.get_my_roles()
      # => [:web]

  """
  @spec add_role(role()) :: :ok
  def add_role(role) do
    NodeConfig.add_role(role)
  end

  @doc """
  Adds multiple roles to the current node.

  All roles are added atomically and propagated to the cluster together.
  Duplicate roles are automatically filtered out.

  ## Parameters

    * `roles` - A list of roles to add (list of atoms)

  ## Returns

  `:ok`

  ## Examples

      ClusterHelper.add_roles([:web, :api, :cache])
      ClusterHelper.get_my_roles()
      # => [:web, :api, :cache]

  """
  @spec add_roles([role()]) :: :ok
  def add_roles(roles) do
    NodeConfig.add_roles(roles)
  end

  @doc """
  Returns all roles assigned to the current node.

  ## Returns

  A list of roles for the current node.

  ## Examples

      ClusterHelper.add_roles([:web, :api])
      ClusterHelper.get_my_roles()
      # => [:web, :api]

  """
  @spec get_my_roles() :: [role()]
  def get_my_roles do
    NodeConfig.get_my_roles()
  end

  @doc """
  Removes a single role from the current node.

  The role is immediately removed locally and the change is propagated
  to all other nodes in the cluster.

  ## Parameters

    * `role` - The role to remove (atom)

  ## Returns

  `:ok`

  ## Examples

      ClusterHelper.add_roles([:web, :api])
      ClusterHelper.remove_role(:api)
      ClusterHelper.get_my_roles()
      # => [:web]

  """
  @spec remove_role(role()) :: :ok
  def remove_role(role) do
    NodeConfig.remove_role(role)
  end

  @doc """
  Check is is local node.

  ## Parameters

    * `node` - A node name.

  ## Returns

  `true|false`

  ## Examples

      ClusterHelper.local_node?(:"node1@127.0.0.1")
      # => true

  """
  @spec local_node?([atom]) :: :ok
  def local_node?(node) do
    NodeConfig.local_node?(node)
  end
end
