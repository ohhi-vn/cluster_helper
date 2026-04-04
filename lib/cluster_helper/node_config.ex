defmodule ClusterHelper.NodeConfig do
  @moduledoc """
  GenServer that owns the ETS role/node table and coordinates cluster sync.

  Supports **overlapping scopes**, allowing a node to participate in multiple
  scopes simultaneously with full isolation between them. A node can have
  different roles in different scopes.

  ## Responsibilities

  - Maintains an ETS `:bag` table keyed on `{:scope, scope, :role, role}` and
    `{:scope, scope, :node, node}` for O(1) lookups in either direction, scoped.
  - Maintains a separate ETS `:set` table for O(1) node enumeration per scope.
  - Registers this node in a `:pg` group **per scope** for isolated messaging.
  - Monitors node connections via `:net_kernel.monitor_nodes/2` (shared across scopes).
  - On startup, pulls the current role state from every already-connected node
    for each active scope.
  - Broadcasts incremental add/remove events scoped to their respective groups.
  - Periodically re-syncs to recover from lost messages or transient failures.
  - Invokes the optional `ClusterHelper.EventHandler` callbacks when roles or
    nodes are first added to the local ETS table.

  ## Internal message protocol

  | Message | Direction | Meaning |
  |---------|-----------|---------|
  | `{:new_roles, scope, roles, node}` | broadcast → handle_info | Remote node added roles in scope |
  | `{:remove_roles, scope, roles, node}` | broadcast → handle_info | Remote node removed roles in scope |
  | `{:nodeup, node, info}` | net_kernel → handle_info | Remote node came online |
  | `{:nodedown, node, info}` | net_kernel → handle_info | Remote node went offline |
  | `{:pull_new_node, scope, node, roles}` | internal Task → handle_info | Pulled roles from a newly discovered node |
  | `{:pull_update_node, scope, node, roles}` | internal Task → handle_info | Full-sync result for a known node |
  | `:pull_roles` | Process.send_after → handle_info | Periodic sync tick |
  """

  use GenServer, restart: :transient

  require Logger

  alias :ets, as: Ets
  alias :pg, as: Pg
  alias ClusterHelper.EventHandler

  @ets_table __MODULE__
  @ets_nodes_table :"#{@ets_table}_nodes"
  @default_interval 7_000
  @default_timeout 5_000

  # ── Client API ────────────────────────────────────────────────────────────────

  @spec start_link(any()) :: GenServer.on_start()
  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @doc """
  Returns all roles assigned to the local node.

  If `scope` is `nil`, returns roles from the default scope.
  """
  @spec get_my_roles(scope :: atom() | nil) :: [ClusterHelper.role()]
  def get_my_roles(scope \\ nil),
    do: GenServer.call(__MODULE__, {:get_my_roles, resolve_scope(scope)})

  @doc """
  Returns every node in the given `scope` that has been assigned `role`.

  If `scope` is `nil`, uses the default scope.
  """
  @spec get_nodes(ClusterHelper.role(), scope :: atom() | nil) :: [node()]
  def get_nodes(role, scope \\ nil) do
    scope = resolve_scope(scope)

    case Ets.lookup(@ets_table, {:scope, scope, :role, role}) do
      [] -> []
      tuples -> for {_, node} <- tuples, do: node
    end
  end

  @doc """
  Returns all roles assigned to `node` in the given `scope`.

  If `scope` is `nil`, uses the default scope.
  """
  @spec get_roles(node(), scope :: atom() | nil) :: [ClusterHelper.role()]
  def get_roles(node, scope \\ nil) do
    scope = resolve_scope(scope)

    case Ets.lookup(@ets_table, {:scope, scope, :node, node}) do
      [] -> []
      tuples -> for {_, role} <- tuples, do: role
    end
  end

  @doc """
  Returns a deduplicated list of every node that has at least one role in the
  given `scope`.

  If `scope` is `nil`, uses the default scope.
  """
  @spec all_nodes(scope :: atom() | nil) :: [node()]
  def all_nodes(scope \\ nil) do
    scope = resolve_scope(scope)
    Ets.select(@ets_nodes_table, [{{{:scope, scope, :"$1"}}, [], [:"$1"]}])
  end

  @doc """
  Adds `role` to the current node in the given `scope` and propagates the change
  cluster-wide.

  If `scope` is `nil`, uses the default scope. The scope is automatically
  joined if it hasn't been already.
  """
  @spec add_role(ClusterHelper.role(), scope :: atom() | nil) :: :ok
  def add_role(role, scope \\ nil),
    do: GenServer.call(__MODULE__, {:add_roles, resolve_scope(scope), [role]})

  @doc """
  Adds each role in `roles` to the current node in the given `scope` and
  propagates cluster-wide.

  If `scope` is `nil`, uses the default scope.
  """
  @spec add_roles([ClusterHelper.role()], scope :: atom() | nil) :: :ok
  def add_roles(roles, scope \\ nil) when is_list(roles),
    do: GenServer.call(__MODULE__, {:add_roles, resolve_scope(scope), roles})

  @doc """
  Removes `role` from the current node in the given `scope` and propagates the
  change cluster-wide.

  If `scope` is `nil`, uses the default scope.
  """
  @spec remove_role(ClusterHelper.role(), scope :: atom() | nil) :: :ok
  def remove_role(role, scope \\ nil),
    do: GenServer.call(__MODULE__, {:remove_roles, resolve_scope(scope), [role]})

  @doc """
  Removes each role in `roles` from the current node in the given `scope` and
  propagates cluster-wide.

  If `scope` is `nil`, uses the default scope.
  """
  @spec remove_roles([ClusterHelper.role()], scope :: atom() | nil) :: :ok
  def remove_roles(roles, scope \\ nil) when is_list(roles),
    do: GenServer.call(__MODULE__, {:remove_roles, resolve_scope(scope), roles})

  @doc """
  Joins the current node to an additional `scope`.

  The scope is started (if not already), the node joins the `:all_nodes` group,
  and an initial pull is performed from all connected nodes for this scope.
  """
  @spec join_scope(atom()) :: :ok
  def join_scope(scope) when is_atom(scope),
    do: GenServer.call(__MODULE__, {:join_scope, scope})

  @doc """
  Leaves the given `scope`, removing all local roles and cleaning up ETS entries
  for this scope.
  """
  @spec leave_scope(atom()) :: :ok
  def leave_scope(scope) when is_atom(scope),
    do: GenServer.call(__MODULE__, {:leave_scope, scope})

  @doc """
  Returns a list of all scopes the local node is currently participating in.
  """
  @spec list_scopes() :: [atom()]
  def list_scopes, do: GenServer.call(__MODULE__, :list_scopes)

  @doc """
  Returns `true` when `node` is the local node, `false` otherwise.
  """
  @spec local_node?(node()) :: boolean()
  def local_node?(node), do: node == Node.self()

  # ── Server callbacks ──────────────────────────────────────────────────────────

  @impl true
  def init(_) do
    # Main role↔node mapping table with compression for memory efficiency
    Ets.new(@ets_table, [
      :bag,
      :named_table,
      :protected,
      :compressed,
      read_concurrency: true,
      write_concurrency: true
    ])

    # Dedicated node tracking table for O(1) enumeration per scope
    Ets.new(@ets_nodes_table, [
      :set,
      :named_table,
      :protected,
      :compressed,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok,
     %{
       scopes: MapSet.new(),
       roles: %{},
       known_nodes: %{}
     }, {:continue, :setup_cluster}}
  end

  @impl true
  def handle_continue(:setup_cluster, state) do
    # Enable node monitoring for nodeup/nodedown events (shared across scopes)
    :net_kernel.monitor_nodes(true, node_type: :visible)

    # Determine initial scopes from config
    configured_scopes =
      case Application.get_env(:cluster_helper, :scopes, nil) do
        nil ->
          # Backward compatibility: single scope config
          [pg_scope()]

        scopes when is_list(scopes) ->
          scopes

        bad ->
          Logger.error("ClusterHelper: :scopes must be a list, got: #{inspect(bad)}")
          [pg_scope()]
      end

    # Join each scope
    state =
      Enum.reduce(configured_scopes, state, fn scope, acc ->
        ensure_scope_started(scope)
        Pg.join(scope, :all_nodes, self())

        %{
          acc
          | scopes: MapSet.put(acc.scopes, scope),
            roles: Map.put_new(acc.roles, scope, MapSet.new()),
            known_nodes: Map.put_new(acc.known_nodes, scope, MapSet.new())
        }
      end)

    # Load initial roles from config (applied to the default scope)
    default_scope = pg_scope()

    roles =
      case Application.get_env(:cluster_helper, :roles, []) do
        roles when is_list(roles) ->
          Logger.info("ClusterHelper starting with roles: #{inspect(roles)} in scope #{inspect(default_scope)}")
          add_my_roles(roles, default_scope)
          MapSet.new(roles)

        bad ->
          Logger.error("ClusterHelper: :roles must be a list, got: #{inspect(bad)}")
          MapSet.new()
      end

    # Defer initial pull to async tasks to avoid deadlock when two nodes
    # connect simultaneously. The periodic pull cycle ensures eventual
    # consistency even if the initial async pull fails.
    Enum.each(state.scopes, fn scope ->
      async_pull_roles_from_cluster(scope)
    end)

    interval = Application.get_env(:cluster_helper, :pull_interval, @default_interval)
    schedule_pull(interval)

    {:noreply, %{state | roles: Map.put(state.roles, default_scope, roles)}}
  end

  # ── Calls ─────────────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:add_roles, scope, new_roles}, _from, state) do
    state = ensure_scope_in_state(state, scope)
    current_roles = Map.get(state.roles, scope, MapSet.new())
    roles_to_add = Enum.reject(new_roles, &MapSet.member?(current_roles, &1))

    if roles_to_add != [] do
      Logger.debug("Adding roles #{inspect(roles_to_add)} to #{inspect(Node.self())} in scope #{inspect(scope)}")
      add_my_roles(roles_to_add, scope)
      broadcast(scope, {:new_roles, scope, roles_to_add, Node.self()})

      new_roles_set = MapSet.union(current_roles, MapSet.new(roles_to_add))
      {:reply, :ok, %{state | roles: Map.put(state.roles, scope, new_roles_set)}}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call({:remove_roles, scope, roles_to_remove}, _from, state) do
    state = ensure_scope_in_state(state, scope)
    current_roles = Map.get(state.roles, scope, MapSet.new())
    roles_to_remove_set = MapSet.new(roles_to_remove)
    roles_remaining = MapSet.difference(current_roles, roles_to_remove_set)

    if MapSet.size(roles_to_remove_set) > 0 do
      Logger.debug("Removing roles #{inspect(roles_to_remove)} from #{inspect(Node.self())} in scope #{inspect(scope)}")

      Enum.each(roles_to_remove, &remove_role_entry_fast(Node.self(), scope, &1))

      broadcast(scope, {:remove_roles, scope, roles_to_remove, Node.self()})
      {:reply, :ok, %{state | roles: Map.put(state.roles, scope, roles_remaining)}}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call({:get_my_roles, scope}, _from, state) do
    roles = Map.get(state.roles, scope, MapSet.new()) |> MapSet.to_list()
    {:reply, roles, state}
  end

  def handle_call({:join_scope, scope}, _from, state) do
    if MapSet.member?(state.scopes, scope) do
      {:reply, {:error, :already_joined}, state}
    else
      ensure_scope_started(scope)
      Pg.join(scope, :all_nodes, self())

      state = %{
        state
        | scopes: MapSet.put(state.scopes, scope),
          roles: Map.put_new(state.roles, scope, MapSet.new()),
          known_nodes: Map.put_new(state.known_nodes, scope, MapSet.new())
      }

      pull_roles_from_cluster(scope)
      {:reply, :ok, state}
    end
  end

  def handle_call({:leave_scope, scope}, _from, state) do
    if MapSet.member?(state.scopes, scope) do
      # Remove all local roles for this scope
      current_roles = Map.get(state.roles, scope, MapSet.new()) |> MapSet.to_list()

      if current_roles != [] do
        Enum.each(current_roles, &remove_role_entry_fast(Node.self(), scope, &1))
      end

      Ets.delete(@ets_nodes_table, {:scope, scope, Node.self()})
      Pg.leave(scope, :all_nodes, self())

      # Clean up all ETS entries for this scope
      Ets.match_delete(@ets_table, {{:scope, scope, :_, :_}, :_})
      Ets.match_delete(@ets_nodes_table, {{:scope, scope, :_}})

      state = %{
        state
        | scopes: MapSet.delete(state.scopes, scope),
          roles: Map.delete(state.roles, scope),
          known_nodes: Map.delete(state.known_nodes, scope)
      }

      {:reply, :ok, state}
    else
      {:reply, {:error, :not_joined}, state}
    end
  end

  def handle_call(:list_scopes, _from, state) do
    {:reply, MapSet.to_list(state.scopes), state}
  end

  # ── Infos ─────────────────────────────────────────────────────────────────────

  @impl true
  def handle_info(:pull_roles, state) do
    Enum.each(state.scopes, fn scope ->
      pull_roles_from_cluster(scope)
    end)

    schedule_pull(Application.get_env(:cluster_helper, :pull_interval, @default_interval))
    {:noreply, state}
  end

  def handle_info({:nodeup, remote_node, _info}, state) do
    if remote_node != Node.self() do
      Logger.debug("Node up: #{inspect(remote_node)}")

      Enum.each(state.scopes, fn scope ->
        known = Map.get(state.known_nodes, scope, MapSet.new())

        if not MapSet.member?(known, remote_node) do
          async_pull_node(scope, remote_node, :pull_new_node)
        end
      end)

      # Mark node as known in all scopes
      new_known =
        Enum.reduce(state.scopes, state.known_nodes, fn scope, acc ->
          Map.update(acc, scope, MapSet.new(), fn nodes ->
            MapSet.put(nodes, remote_node)
          end)
        end)

      {:noreply, %{state | known_nodes: new_known}}
    else
      {:noreply, state}
    end
  end

  def handle_info({:nodedown, remote_node, _info}, state) do
    Logger.debug("Node down: #{inspect(remote_node)}")

    Enum.each(state.scopes, fn scope ->
      remove_node_entry(remote_node, scope)
    end)

    # Remove node from known set in all scopes
    new_known =
      Enum.reduce(state.scopes, state.known_nodes, fn scope, acc ->
        Map.update(acc, scope, MapSet.new(), fn nodes ->
          MapSet.delete(nodes, remote_node)
        end)
      end)

    {:noreply, %{state | known_nodes: new_known}}
  end

  def handle_info({:new_roles, scope, roles, remote_node}, state) do
    if remote_node != Node.self() and MapSet.member?(state.scopes, scope) do
      Logger.debug("Received new roles #{inspect(roles)} from #{inspect(remote_node)} in scope #{inspect(scope)}")
      add_roles_for_node(remote_node, scope, roles)

      new_known =
        Map.update(state.known_nodes, scope, MapSet.new(), fn nodes ->
          MapSet.put(nodes, remote_node)
        end)

      {:noreply, %{state | known_nodes: new_known}}
    else
      {:noreply, state}
    end
  end

  def handle_info({:remove_roles, scope, roles, remote_node}, state) do
    if remote_node != Node.self() and MapSet.member?(state.scopes, scope) do
      Logger.debug("Removing roles #{inspect(roles)} from #{inspect(remote_node)} in scope #{inspect(scope)}")
      Enum.each(roles, &remove_role_entry_fast(remote_node, scope, &1))

      case Ets.lookup(@ets_table, {:scope, scope, :node, remote_node}) do
        [] -> Ets.delete(@ets_nodes_table, {:scope, scope, remote_node})
        _ -> :ok
      end
    end

    {:noreply, state}
  end

  def handle_info({:pull_update_node, scope, remote_node, roles}, state) do
    remove_node_entry(remote_node, scope)
    add_roles_for_node(remote_node, scope, roles)

    new_known =
      Map.update(state.known_nodes, scope, MapSet.new(), fn nodes ->
        MapSet.put(nodes, remote_node)
      end)

    {:noreply, %{state | known_nodes: new_known}}
  end

  def handle_info({:pull_new_node, scope, remote_node, roles}, state) do
    EventHandler.dispatch_node_added(remote_node)
    add_roles_for_node(remote_node, scope, roles)

    new_known =
      Map.update(state.known_nodes, scope, MapSet.new(), fn nodes ->
        MapSet.put(nodes, remote_node)
      end)

    {:noreply, %{state | known_nodes: new_known}}
  end

  def handle_info({:pull_complete, scope}, state) do
    Logger.debug("Initial pull complete for scope #{inspect(scope)}")
    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.debug("ClusterHelper.NodeConfig received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  # ── Private helpers ───────────────────────────────────────────────────────────

  defp ensure_scope_in_state(state, scope) do
    if MapSet.member?(state.scopes, scope) do
      state
    else
      Logger.info("Auto-joining scope #{inspect(scope)}")
      ensure_scope_started(scope)
      Pg.join(scope, :all_nodes, self())

      %{
        state
        | scopes: MapSet.put(state.scopes, scope),
          roles: Map.put_new(state.roles, scope, MapSet.new()),
          known_nodes: Map.put_new(state.known_nodes, scope, MapSet.new())
      }
    end
  end

  defp ensure_scope_started(scope) do
    :pg.start(scope)
  end

  defp add_my_roles(roles, scope) do
    if roles == [] do
      :ok
    else
      node = Node.self()

      # Individual inserts are much faster than batch for :bag tables
      Enum.each(roles, fn role ->
        Ets.insert(@ets_table, {{:scope, scope, :role, role}, node})
        Ets.insert(@ets_table, {{:scope, scope, :node, node}, role})
      end)

      Ets.insert(@ets_nodes_table, {{:scope, scope, node}})

      # Fire callbacks synchronously for correctness
      Enum.each(roles, fn role ->
        EventHandler.dispatch_role_added(node, role)
      end)
    end
  end

  defp add_roles_for_node(node, scope, roles) do
    if roles == [] do
      :ok
    else
      # Individual inserts are much faster than batch for :bag tables
      Enum.each(roles, fn role ->
        Ets.insert(@ets_table, {{:scope, scope, :role, role}, node})
        Ets.insert(@ets_table, {{:scope, scope, :node, node}, role})
      end)

      Ets.insert(@ets_nodes_table, {{:scope, scope, node}})

      # Fire callbacks synchronously for correctness
      Enum.each(roles, fn role ->
        EventHandler.dispatch_role_added(node, role)
      end)
    end
  end

  defp remove_role_entry_fast(node, scope, role) do
    Ets.delete_object(@ets_table, {{:scope, scope, :role, role}, node})
    Ets.delete_object(@ets_table, {{:scope, scope, :node, node}, role})

    # Clean up node entry if no more roles remain for this node in this scope
    case Ets.lookup(@ets_table, {:scope, scope, :node, node}) do
      [] -> Ets.delete(@ets_nodes_table, {:scope, scope, node})
      _ -> :ok
    end
  end

  defp remove_node_entry(node, scope) do
    Ets.match_delete(@ets_table, {{:scope, scope, :node, node}, :_})
    Ets.match_delete(@ets_table, {{:scope, scope, :role, :_}, node})
    Ets.delete(@ets_nodes_table, {:scope, scope, node})
  end

  defp broadcast(scope, message) do
    local_pid = self()

    scope
    |> Pg.get_members(:all_nodes)
    |> Enum.each(fn pid ->
      if pid != local_pid do
        send(pid, message)
      end
    end)
  end

  defp async_pull_node(scope, remote_node, msg_tag) do
    server = self()

    Task.start(fn ->
      Logger.debug("Pulling roles from #{inspect(remote_node)} for scope #{inspect(scope)}")

      case pull_roles_from_node(scope, remote_node) do
        {:ok, roles} ->
          send(server, {msg_tag, scope, remote_node, roles})

        {:error, reason} ->
          Logger.warning(
            "ClusterHelper: failed to pull from #{inspect(remote_node)}: #{inspect(reason)}"
          )
      end
    end)
  end

  # Async wrapper for pull_roles_from_cluster to avoid blocking handle_continue.
  defp async_pull_roles_from_cluster(scope) do
    server = self()

    Task.start(fn ->
      pull_roles_from_cluster(scope)
      send(server, {:pull_complete, scope})
    end)
  end

  defp pull_roles_from_cluster(scope) do
    current_node = Node.self()

    live_nodes =
      Node.list()
      |> Enum.reject(&(&1 == current_node))

    Enum.each(live_nodes, &async_pull_node(scope, &1, :pull_update_node))

    stale_nodes = all_nodes(scope) -- [current_node | live_nodes]

    Enum.each(stale_nodes, fn node ->
      Logger.info("ClusterHelper: node #{inspect(node)} left scope #{inspect(scope)}, removing roles")
      remove_node_entry(node, scope)
    end)
  end


  # No async dispatch helpers needed - callbacks are synchronous for correctness.

  defp pull_roles_from_node(scope, node) do
    timeout = Application.get_env(:cluster_helper, :pull_timeout, @default_timeout)
    do_pull_with_retry(scope, node, timeout, 10)
  end

  # Retry :erpc.call a few times to handle :noproc during peer startup.
  # Uses a short timeout for retries since :noproc means the GenServer
  # isn't ready yet - no point waiting the full timeout.
  defp do_pull_with_retry(scope, node, timeout, retries) do
    # Use shorter timeout for retry attempts
    retry_timeout = if retries < 10, do: min(timeout, 1000), else: timeout

    result =
      try do
        :erpc.call(node, ClusterHelper, :get_my_roles, [scope], retry_timeout)
      catch
        :exit, reason -> {:error, reason}
      end

    case result do
      {:error, {:exception, {:noproc, _}}} when retries > 1 ->
        Process.sleep(500)
        do_pull_with_retry(scope, node, timeout, retries - 1)

      {:error, {:noproc, _}} when retries > 1 ->
        Process.sleep(500)
        do_pull_with_retry(scope, node, timeout, retries - 1)

      {:error, reason} ->
        {:error, reason}

      roles when is_list(roles) ->
        {:ok, roles}

      other ->
        Logger.debug("Unexpected pull result from #{inspect(node)}: #{inspect(other)}")
        {:error, other}
    end
  end

  defp schedule_pull(interval), do: Process.send_after(self(), :pull_roles, interval)

  defp pg_scope, do: Application.get_env(:cluster_helper, :scope, ClusterHelper)

  defp resolve_scope(nil), do: pg_scope()
  defp resolve_scope(scope) when is_atom(scope), do: scope
end
