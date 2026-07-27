defmodule ClusterHelper.ETSStore do
  @behaviour ClusterHelper.RoleStore

  @moduledoc """
  ETS-based implementation of `ClusterHelper.RoleStore`.

  Maintains two named ETS tables:
    * `ClusterHelper.NodeConfig` — a `:bag` table with `{:scope, scope, :role, role} → node`
      and `{:scope, scope, :node, node} → role` entries for bidirectional O(1) lookups
    * `ClusterHelper.NodeConfig_nodes` — a `:set` table with `{:scope, scope, node}` entries
      for O(1) node enumeration per scope

  Read operations (`get_my_roles/1`, `get_nodes/2`, `get_roles/2`, `all_nodes/1`)
  bypass any GenServer and read directly from ETS with `read_concurrency: true`.

  ## Table names

  Table names are kept as `ClusterHelper.NodeConfig` and its derived name
  for backward compatibility with any code that references the ETS table
  directly.
  """

  alias ClusterHelper.EventHandler

  @ets_table ClusterHelper.NodeConfig
  @ets_nodes_table :"Elixir.ClusterHelper.NodeConfig_nodes"

  @impl true
  def init do
    :ets.new(@ets_table, [
      :bag,
      :named_table,
      :public,
      :compressed,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ets.new(@ets_nodes_table, [
      :set,
      :named_table,
      :public,
      :compressed,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ok
  end

  @impl true
  def get_my_roles(scope) do
    pattern = {{:scope, scope, :node, Node.self()}, :"$1"}
    :ets.select(@ets_table, [{pattern, [], [:"$1"]}])
  end

  @impl true
  def get_nodes(role, scope) do
    pattern = {{:scope, scope, :role, role}, :"$1"}
    :ets.select(@ets_table, [{pattern, [], [:"$1"]}])
  end

  @impl true
  def get_roles(node, scope) do
    pattern = {{:scope, scope, :node, node}, :"$1"}
    :ets.select(@ets_table, [{pattern, [], [:"$1"]}])
  end

  @impl true
  def all_nodes(scope) do
    :ets.select(@ets_nodes_table, [{{{:scope, scope, :"$1"}}, [], [:"$1"]}])
  end

  @impl true
  def insert_roles(node, scope, roles) when is_list(roles) do
    if roles != [] do
      entries =
        Enum.flat_map(roles, fn role ->
          [
            {{:scope, scope, :role, role}, node},
            {{:scope, scope, :node, node}, role}
          ]
        end)

      :ets.insert(@ets_table, entries)
      :ets.insert(@ets_nodes_table, {{:scope, scope, node}})

      Enum.each(roles, &EventHandler.dispatch_role_added(node, &1))
    end

    :ok
  end

  @impl true
  def delete_role(node, scope, role) do
    :ets.delete_object(@ets_table, {{:scope, scope, :role, role}, node})
    :ets.delete_object(@ets_table, {{:scope, scope, :node, node}, role})

    EventHandler.dispatch_role_removed(node, role)

    if :ets.lookup(@ets_table, {:scope, scope, :node, node}) == [] do
      :ets.delete(@ets_nodes_table, {:scope, scope, node})
    end

    :ok
  end

  @impl true
  def delete_node(node, scope) do
    current_roles =
      case :ets.lookup(@ets_table, {:scope, scope, :node, node}) do
        [] -> []
        tuples -> for {_, role} <- tuples, do: role
      end

    :ets.match_delete(@ets_table, {{:scope, scope, :node, node}, :_})
    :ets.match_delete(@ets_table, {{:scope, scope, :role, :_}, node})
    :ets.delete(@ets_nodes_table, {:scope, scope, node})

    Enum.each(current_roles, &EventHandler.dispatch_role_removed(node, &1))

    :ok
  end

  @doc """
  Purges all ETS entries for a scope.
  """
  @spec purge_scope(atom()) :: :ok
  def purge_scope(scope) do
    :ets.match_delete(@ets_table, {{:scope, scope, :_, :_}, :_})
    :ets.match_delete(@ets_nodes_table, {{:scope, scope, :_}})
    :ok
  end

  @doc """
  Returns `true` if the node has any roles in the scope.
  """
  @spec has_roles?(node(), atom()) :: boolean()
  def has_roles?(node, scope) do
    :ets.lookup(@ets_table, {:scope, scope, :node, node}) != []
  end
end
