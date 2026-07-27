defmodule ClusterHelper.RoleStore do
  @moduledoc """
  Port (behaviour) for role data storage.

  Implementations provide persistent storage for role↔node mappings
  and node membership tracking per scope.

  The default implementation is `ClusterHelper.ETSStore`.
  """

  @type role :: term()
  @type node_name :: node()
  @type scope :: atom()

  @doc "Initialises the store (creates tables, etc.)."
  @callback init() :: :ok

  @doc "Returns all roles for the local node in the given scope."
  @callback get_my_roles(scope()) :: [role()]

  @doc "Returns all nodes that have the given role in the scope."
  @callback get_nodes(role(), scope()) :: [node_name()]

  @doc "Returns all roles for the given node in the scope."
  @callback get_roles(node_name(), scope()) :: [role()]

  @doc "Returns all nodes that have at least one role in the scope."
  @callback all_nodes(scope()) :: [node_name()]

  @doc "Inserts roles for a node in a scope."
  @callback insert_roles(node_name(), scope(), [role()]) :: :ok

  @doc "Removes a single role from a node in a scope."
  @callback delete_role(node_name(), scope(), role()) :: :ok

  @doc "Removes all role entries for a node in a scope."
  @callback delete_node(node_name(), scope()) :: :ok
end
