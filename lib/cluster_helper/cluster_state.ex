defmodule ClusterHelper.ClusterState do
  @moduledoc """
  Pure domain state transformations for cluster membership.

  All functions in this module are **pure** — they take state and return
  new state with no side effects. This keeps the core business logic
  (scopes, roles, generations) testable and independent of GenServer or ETS.
  """

  @type role_set :: MapSet.t(term())
  @type node_set :: MapSet.t(node())
  @type gen_map :: %{node() => integer()}

  @type t :: %{
          optional(:scopes) => MapSet.t(atom()),
          optional(:roles) => %{atom() => role_set},
          optional(:known_nodes) => %{atom() => node_set},
          optional(:generations) => %{atom() => integer()},
          optional(:remote_generations) => %{atom() => gen_map}
        }

  @doc """
  Returns a new empty state.
  """
  @spec new() :: t()
  def new do
    %{
      scopes: MapSet.new(),
      roles: %{},
      known_nodes: %{},
      generations: %{},
      remote_generations: %{}
    }
  end

  @doc """
  Adds a scope to the state, initialising all per-scope maps.
  """
  @spec add_scope(t(), atom()) :: t()
  def add_scope(state, scope) do
    state
    |> put_new(:roles, scope, MapSet.new())
    |> put_new(:known_nodes, scope, MapSet.new())
    |> put_new(:generations, scope, 0)
    |> put_new(:remote_generations, scope, %{})
    |> put_in([:scopes], MapSet.put(state.scopes, scope))
  end

  @doc """
  Removes a scope and all its data from the state.
  """
  @spec remove_scope(t(), atom()) :: t()
  def remove_scope(state, scope) do
    %{
      state
      | scopes: MapSet.delete(state.scopes, scope),
        roles: Map.delete(state.roles, scope),
        known_nodes: Map.delete(state.known_nodes, scope),
        generations: Map.delete(state.generations, scope),
        remote_generations: Map.delete(state.remote_generations, scope)
    }
  end

  @doc """
  Adds roles to a scope, returning updated state and the roles that were
  actually new (not already present).
  """
  @spec add_roles(t(), atom(), [term()]) :: {t(), [term()], integer()}
  def add_roles(state, scope, new_roles) do
    current = Map.get(state.roles, scope, MapSet.new())
    to_add = Enum.reject(new_roles, &MapSet.member?(current, &1))

    if to_add == [] do
      {state, [], Map.get(state.generations, scope, 0)}
    else
      updated = MapSet.union(current, MapSet.new(to_add))
      gen = Map.get(state.generations, scope, 0) + 1

      {%{
         state
         | roles: Map.put(state.roles, scope, updated),
           generations: Map.put(state.generations, scope, gen)
       }, to_add, gen}
    end
  end

  @doc """
  Removes roles from a scope, returning updated state and the roles that
  were actually removed (existed in the set).
  """
  @spec remove_roles(t(), atom(), [term()]) :: {t(), [term()], integer()}
  def remove_roles(state, scope, roles_to_remove) do
    current = Map.get(state.roles, scope, MapSet.new())
    remove_set = MapSet.new(roles_to_remove)
    actually_removed = MapSet.intersection(current, remove_set)

    if MapSet.size(actually_removed) == 0 do
      {state, [], Map.get(state.generations, scope, 0)}
    else
      removed_list = MapSet.to_list(actually_removed)
      remaining = MapSet.difference(current, actually_removed)
      gen = Map.get(state.generations, scope, 0) + 1

      {%{
         state
         | roles: Map.put(state.roles, scope, remaining),
           generations: Map.put(state.generations, scope, gen)
       }, removed_list, gen}
    end
  end

  @doc """
  Marks a node as known in a scope.
  """
  @spec add_known_node(t(), atom(), node()) :: t()
  def add_known_node(state, scope, node) do
    update_in(state, [:known_nodes, scope], fn nodes ->
      MapSet.put(nodes || MapSet.new(), node)
    end)
  end

  @doc """
  Removes a node from the known set in a scope.
  """
  @spec remove_known_node(t(), atom(), node()) :: t()
  def remove_known_node(state, scope, node) do
    update_in(state, [:known_nodes, scope], fn nodes ->
      MapSet.delete(nodes || MapSet.new(), node)
    end)
  end

  @doc """
  Removes a node from all known-node sets across all scopes.
  """
  @spec remove_known_node_from_all(t(), node()) :: t()
  def remove_known_node_from_all(state, node) do
    Enum.reduce(state.known_nodes, state, fn {scope, nodes}, acc ->
      if MapSet.member?(nodes, node) do
        put_in(acc, [:known_nodes, scope], MapSet.delete(nodes, node))
      else
        acc
      end
    end)
  end

  @doc """
  Removes a node's remote generation from all scopes.
  """
  @spec remove_remote_gen_from_all(t(), node()) :: t()
  def remove_remote_gen_from_all(state, node) do
    Enum.reduce(state.remote_generations, state, fn {scope, gens}, acc ->
      put_in(acc, [:remote_generations, scope], Map.delete(gens, node))
    end)
  end

  @doc """
  Updates the remote generation for a node in a scope.
  """
  @spec put_remote_generation(t(), atom(), node(), integer()) :: t()
  def put_remote_generation(state, scope, node, gen) do
    put_in(state, [:remote_generations, scope, node], gen)
  end

  @doc """
  Returns `true` if the scope exists in the state.
  """
  @spec has_scope?(t(), atom()) :: boolean()
  def has_scope?(state, scope) do
    MapSet.member?(state.scopes, scope)
  end

  defp put_new(state, key, scope, default) do
    Map.update(state, key, %{scope => default}, &Map.put_new(&1, scope, default))
  end
end
