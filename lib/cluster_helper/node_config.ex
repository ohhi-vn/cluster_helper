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
  - Broadcasts incremental add/remove events scoped to their respective groups
    using `:pg.get_members/2` + `send/2` for efficient delivery.
  - Periodically re-syncs using **generation-based change detection**: each scope
    tracks a monotonically increasing generation counter that increments on every
    local role change. Remote nodes are only fully pulled when their generation
    has changed, dramatically reducing unnecessary RPC traffic.
  - Invokes the optional `ClusterHelper.EventHandler` callbacks when roles or
    nodes are added or removed from the local ETS table.

  ## Performance characteristics

  - **Read operations** (`get_nodes/2`, `get_roles/2`, `all_nodes/1`) bypass the
    GenServer and read directly from ETS tables, which are `:protected` with
    `read_concurrency: true`. This provides microsecond-level lookups without
    GenServer bottleneck.
  - **Batch ETS inserts** are used for bulk role additions, significantly reducing
    the overhead of adding multiple roles at once.
  - **Task.Supervisor** (`ClusterHelper.TaskSupervisor`) is used for all async
    pull tasks, providing better fault tolerance and back-pressure compared to
    bare `Task.start/1`.
  - **`:pg.get_members/2` + `send/2`** is used for cluster-wide event broadcasting,
    leveraging the VM's native process group membership for efficient delivery.

  ## Generation-based sync

  Each scope maintains a local `generation` counter (starting at 0) that is
  incremented every time roles are added or removed on this node. During the
  periodic pull, the GenServer first checks the remote node's generation via a
  lightweight `:erpc.call` to `__get_generation__/1`. Only if the generation has
  changed (or the node is newly discovered) does a full role pull occur. This
  avoids expensive full-sync RPCs when nothing has changed.

  ## Smart node up/down handling

  On `:nodeup`, the GenServer discovers which scopes the new node participates
  in by calling `__get_scopes__/0` on the remote node. It then only pulls roles
  for matching scopes, avoiding unnecessary cross-scope RPCs. The `on_node_added`
  callback fires for each scope where the node is discovered.

  On `:nodedown`, the node is removed from all scopes and the `on_node_removed`
  callback is fired.

  ## Internal message protocol

  | Message | Direction | Meaning |
  |---------|-----------|---------|
  | `{:new_roles, scope, roles, node}` | broadcast → handle_info | Remote node added roles in scope |
  | `{:remove_roles, scope, roles, node}` | broadcast → handle_info | Remote node removed roles in scope |
  | `{:nodeup, node, info}` | net_kernel → handle_info | Remote node came online |
  | `{:nodedown, node, info}` | net_kernel → handle_info | Remote node went offline |
  | `{:pull_new_node, scope, node, roles}` | Task → handle_info | Pulled roles from a newly discovered node |
  | `{:pull_new_node, scope, node, roles, gen}` | Task → handle_info | Same, with generation metadata |
  | `{:pull_update_node, scope, node, roles}` | Task → handle_info | Full-sync result for a known node |
  | `{:pull_update_node, scope, node, roles, gen}` | Task → handle_info | Same, with generation metadata |
  | `{:stale_node, scope, node}` | Task → handle_info | Node left the cluster, clean up |
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
  @task_supervisor ClusterHelper.TaskSupervisor

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

  Reads directly from ETS for microsecond-level lookups without GenServer overhead.
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

  Reads directly from ETS for microsecond-level lookups without GenServer overhead.
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

  Reads directly from ETS for microsecond-level lookups without GenServer overhead.
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
  @spec join_scope(atom()) :: :ok | {:error, :already_joined}
  def join_scope(scope) when is_atom(scope),
    do: GenServer.call(__MODULE__, {:join_scope, scope})

  @doc """
  Leaves the given `scope`, removing all local roles and cleaning up ETS entries
  for this scope.
  """
  @spec leave_scope(atom()) :: :ok | {:error, :not_joined}
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

  @doc false
  @spec __get_generation__(atom()) :: integer()
  def __get_generation__(scope) do
    GenServer.call(__MODULE__, {:__get_generation__, scope})
  end

  @doc false
  @spec __get_scopes__() :: [atom()]
  def __get_scopes__() do
    GenServer.call(__MODULE__, :__get_scopes__)
  end

  # ── Server callbacks ──────────────────────────────────────────────────────────

  @impl true
  def init(_) do
    Ets.new(@ets_table, [
      :bag,
      :named_table,
      :protected,
      :compressed,
      read_concurrency: true,
      write_concurrency: true
    ])

    Ets.new(@ets_nodes_table, [
      :set,
      :named_table,
      :protected,
      :compressed,
      read_concurrency: true,
      write_concurrency: true
    ])

    ensure_task_supervisor()

    {:ok,
     %{
       scopes: MapSet.new(),
       roles: %{},
       known_nodes: %{},
       generations: %{},
       remote_generations: %{}
     }, {:continue, :setup_cluster}}
  end

  @impl true
  def handle_continue(:setup_cluster, state) do
    :net_kernel.monitor_nodes(true, node_type: :visible)

    configured_scopes =
      case Application.get_env(:cluster_helper, :scopes, nil) do
        nil ->
          [pg_scope()]

        scopes when is_list(scopes) ->
          scopes

        bad ->
          Logger.error("ClusterHelper: :scopes must be a list, got: #{inspect(bad)}")
          [pg_scope()]
      end

    state =
      Enum.reduce(configured_scopes, state, fn scope, acc ->
        ensure_scope_started(scope)
        Pg.join(scope, :all_nodes, self())

        %{
          acc
          | scopes: MapSet.put(acc.scopes, scope),
            roles: Map.put_new(acc.roles, scope, MapSet.new()),
            known_nodes: Map.put_new(acc.known_nodes, scope, MapSet.new()),
            generations: Map.put_new(acc.generations, scope, 0),
            remote_generations: Map.put_new(acc.remote_generations, scope, %{})
        }
      end)

    default_scope = pg_scope()

    {roles, initial_gen} =
      case Application.get_env(:cluster_helper, :roles, []) do
        roles when is_list(roles) and roles != [] ->
          Logger.info(
            "ClusterHelper starting with roles: #{inspect(roles)} in scope #{inspect(default_scope)}"
          )

          add_my_roles(roles, default_scope)
          {MapSet.new(roles), 1}

        roles when is_list(roles) ->
          {MapSet.new(), 0}

        bad ->
          Logger.error("ClusterHelper: :roles must be a list, got: #{inspect(bad)}")
          {MapSet.new(), 0}
      end

    Enum.each(state.scopes, fn scope ->
      async_pull_roles_from_cluster(scope)
    end)

    interval = Application.get_env(:cluster_helper, :pull_interval, @default_interval)
    schedule_pull(interval)

    {:noreply,
     %{
       state
       | roles: Map.put(state.roles, default_scope, roles),
         generations: Map.put(state.generations, default_scope, initial_gen)
     }}
  end

  # ── Calls ─────────────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:add_roles, scope, new_roles}, _from, state) do
    state = ensure_scope_in_state(state, scope)
    current_roles = Map.get(state.roles, scope, MapSet.new())
    roles_to_add = Enum.reject(new_roles, &MapSet.member?(current_roles, &1))

    if roles_to_add != [] do
      Logger.debug(
        "Adding roles #{inspect(roles_to_add)} to #{inspect(Node.self())} in scope #{inspect(scope)}"
      )

      add_my_roles(roles_to_add, scope)
      broadcast(scope, {:new_roles, scope, roles_to_add, Node.self()})

      new_roles_set = MapSet.union(current_roles, MapSet.new(roles_to_add))
      new_gen = Map.get(state.generations, scope, 0) + 1

      {:reply, :ok,
       %{
         state
         | roles: Map.put(state.roles, scope, new_roles_set),
           generations: Map.put(state.generations, scope, new_gen)
       }}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call({:remove_roles, scope, roles_to_remove}, _from, state) do
    state = ensure_scope_in_state(state, scope)
    current_roles = Map.get(state.roles, scope, MapSet.new())
    roles_to_remove_set = MapSet.new(roles_to_remove)
    # Only process roles that actually exist — avoids spurious callbacks and
    # generation bumps when removing non-existent roles.
    roles_actually_removed = MapSet.intersection(current_roles, roles_to_remove_set)

    if MapSet.size(roles_actually_removed) > 0 do
      Logger.debug(
        "Removing roles #{inspect(MapSet.to_list(roles_actually_removed))} from #{inspect(Node.self())} in scope #{inspect(scope)}"
      )

      Enum.each(roles_actually_removed, &remove_role_entry_fast(Node.self(), scope, &1))

      broadcast(
        scope,
        {:remove_roles, scope, MapSet.to_list(roles_actually_removed), Node.self()}
      )

      roles_remaining = MapSet.difference(current_roles, roles_actually_removed)
      new_gen = Map.get(state.generations, scope, 0) + 1

      {:reply, :ok,
       %{
         state
         | roles: Map.put(state.roles, scope, roles_remaining),
           generations: Map.put(state.generations, scope, new_gen)
       }}
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
          known_nodes: Map.put_new(state.known_nodes, scope, MapSet.new()),
          generations: Map.put_new(state.generations, scope, 0),
          remote_generations: Map.put_new(state.remote_generations, scope, %{})
      }

      pull_roles_from_cluster(scope)
      {:reply, :ok, state}
    end
  end

  def handle_call({:leave_scope, scope}, _from, state) do
    if MapSet.member?(state.scopes, scope) do
      current_roles = Map.get(state.roles, scope, MapSet.new()) |> MapSet.to_list()

      if current_roles != [] do
        Enum.each(current_roles, &remove_role_entry_fast(Node.self(), scope, &1))
      end

      Ets.delete(@ets_nodes_table, {:scope, scope, Node.self()})
      Pg.leave(scope, :all_nodes, self())

      Ets.match_delete(@ets_table, {{:scope, scope, :_, :_}, :_})
      Ets.match_delete(@ets_nodes_table, {{:scope, scope, :_}})

      state = %{
        state
        | scopes: MapSet.delete(state.scopes, scope),
          roles: Map.delete(state.roles, scope),
          known_nodes: Map.delete(state.known_nodes, scope),
          generations: Map.delete(state.generations, scope),
          remote_generations: Map.delete(state.remote_generations, scope)
      }

      {:reply, :ok, state}
    else
      {:reply, {:error, :not_joined}, state}
    end
  end

  def handle_call(:list_scopes, _from, state) do
    {:reply, MapSet.to_list(state.scopes), state}
  end

  def handle_call({:__get_generation__, scope}, _from, state) do
    gen = Map.get(state.generations, scope, 0)
    {:reply, gen, state}
  end

  def handle_call(:__get_scopes__, _from, state) do
    {:reply, MapSet.to_list(state.scopes), state}
  end

  # ── Infos ─────────────────────────────────────────────────────────────────────

  @impl true
  def handle_info(:pull_roles, state) do
    Enum.each(state.scopes, fn scope ->
      async_pull_roles_from_cluster_with_generation_check(scope, state)
    end)

    schedule_pull(Application.get_env(:cluster_helper, :pull_interval, @default_interval))
    {:noreply, state}
  end

  def handle_info({:nodeup, remote_node, _info}, state) do
    if remote_node != Node.self() do
      Logger.debug("Node up: #{inspect(remote_node)}")

      server = self()
      local_scopes = state.scopes

      Task.Supervisor.start_child(@task_supervisor, fn ->
        # Discover which scopes the remote node participates in
        remote_scopes =
          try do
            :erpc.call(remote_node, ClusterHelper.NodeConfig, :__get_scopes__, [], 2000)
          catch
            :exit, reason ->
              Logger.warning(
                "Failed to get scopes from #{inspect(remote_node)}, reason: #{inspect(reason)}"
              )

              []

            :error, :undef ->
              Logger.debug("MFA not found on #{inspect(remote_node)}, skipped")
              []
          end

        matching_scopes = Enum.filter(remote_scopes, &MapSet.member?(local_scopes, &1))

        # Pull roles for each matching scope
        Enum.each(matching_scopes, fn scope ->
          case pull_roles_from_node(scope, remote_node) do
            {:ok, roles} ->
              gen =
                case get_remote_generation(remote_node, scope) do
                  {:ok, g} -> g
                  {:error, _} -> nil
                end

              if gen do
                send(server, {:pull_new_node, scope, remote_node, roles, gen})
              else
                send(server, {:pull_new_node, scope, remote_node, roles})
              end

            {:error, reason} ->
              Logger.warning(
                "ClusterHelper: failed to pull from #{inspect(remote_node)}: #{inspect(reason)}"
              )
          end
        end)
      end)

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  def handle_info({:nodedown, remote_node, _info}, state) do
    Logger.debug("Node down: #{inspect(remote_node)}")

    Enum.each(state.scopes, fn scope ->
      remove_node_entry(remote_node, scope)
    end)

    dispatch_node_removed(remote_node)

    new_known =
      Enum.reduce(state.scopes, state.known_nodes, fn scope, acc ->
        Map.update(acc, scope, MapSet.new(), &MapSet.delete(&1, remote_node))
      end)

    new_remote_gens =
      Enum.reduce(state.scopes, state.remote_generations, fn scope, acc ->
        Map.update(acc, scope, %{}, &Map.delete(&1, remote_node))
      end)

    {:noreply, %{state | known_nodes: new_known, remote_generations: new_remote_gens}}
  end

  def handle_info({:new_roles, scope, roles, remote_node}, state) do
    if remote_node != Node.self() and MapSet.member?(state.scopes, scope) do
      Logger.debug(
        "Received new roles #{inspect(roles)} from #{inspect(remote_node)} in scope #{inspect(scope)}"
      )

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
      Logger.debug(
        "Removing roles #{inspect(roles)} from #{inspect(remote_node)} in scope #{inspect(scope)}"
      )

      Enum.each(roles, &remove_role_entry_fast(remote_node, scope, &1))

      case Ets.lookup(@ets_table, {:scope, scope, :node, remote_node}) do
        [] -> Ets.delete(@ets_nodes_table, {:scope, scope, remote_node})
        _ -> :ok
      end
    end

    {:noreply, state}
  end

  # Handle both old format (without generation) and new format (with generation)
  def handle_info({:pull_update_node, scope, remote_node, roles}, state) do
    do_handle_pull_update_node(scope, remote_node, roles, nil, state)
  end

  def handle_info({:pull_update_node, scope, remote_node, roles, new_gen}, state) do
    do_handle_pull_update_node(scope, remote_node, roles, new_gen, state)
  end

  def handle_info({:pull_new_node, scope, remote_node, roles}, state) do
    do_handle_pull_new_node(scope, remote_node, roles, nil, state)
  end

  def handle_info({:pull_new_node, scope, remote_node, roles, new_gen}, state) do
    do_handle_pull_new_node(scope, remote_node, roles, new_gen, state)
  end

  def handle_info({:stale_node, scope, node}, state) do
    Logger.info(
      "ClusterHelper: node #{inspect(node)} left scope #{inspect(scope)}, removing roles"
    )

    remove_node_entry(node, scope)

    new_known =
      Map.update(state.known_nodes, scope, MapSet.new(), &MapSet.delete(&1, node))

    new_remote_gens =
      Map.update(state.remote_generations, scope, %{}, &Map.delete(&1, node))

    {:noreply, %{state | known_nodes: new_known, remote_generations: new_remote_gens}}
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

  defp do_handle_pull_update_node(scope, remote_node, roles, new_gen, state) do
    remove_node_entry(remote_node, scope)
    add_roles_for_node(remote_node, scope, roles)

    new_known =
      Map.update(state.known_nodes, scope, MapSet.new(), &MapSet.put(&1, remote_node))

    new_remote_gens =
      if new_gen do
        Map.update(state.remote_generations, scope, %{}, &Map.put(&1, remote_node, new_gen))
      else
        state.remote_generations
      end

    {:noreply, %{state | known_nodes: new_known, remote_generations: new_remote_gens}}
  end

  defp do_handle_pull_new_node(scope, remote_node, roles, new_gen, state) do
    EventHandler.dispatch_node_added(remote_node)
    add_roles_for_node(remote_node, scope, roles)

    new_known =
      Map.update(state.known_nodes, scope, MapSet.new(), &MapSet.put(&1, remote_node))

    new_remote_gens =
      if new_gen do
        Map.update(state.remote_generations, scope, %{}, &Map.put(&1, remote_node, new_gen))
      else
        state.remote_generations
      end

    {:noreply, %{state | known_nodes: new_known, remote_generations: new_remote_gens}}
  end

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
          known_nodes: Map.put_new(state.known_nodes, scope, MapSet.new()),
          generations: Map.put_new(state.generations, scope, 0),
          remote_generations: Map.put_new(state.remote_generations, scope, %{})
      }
    end
  end

  defp ensure_scope_started(scope) do
    :pg.start(scope)
  end

  defp ensure_task_supervisor do
    case Process.whereis(@task_supervisor) do
      nil ->
        case Task.Supervisor.start_link(name: @task_supervisor) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
        end

      _pid ->
        :ok
    end
  end

  defp add_my_roles(roles, scope) do
    if roles == [] do
      :ok
    else
      node = Node.self()

      # Batch insert for significantly better performance
      entries =
        Enum.flat_map(roles, fn role ->
          [
            {{:scope, scope, :role, role}, node},
            {{:scope, scope, :node, node}, role}
          ]
        end)

      Ets.insert(@ets_table, entries)
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
      # Batch insert for significantly better performance
      entries =
        Enum.flat_map(roles, fn role ->
          [
            {{:scope, scope, :role, role}, node},
            {{:scope, scope, :node, node}, role}
          ]
        end)

      Ets.insert(@ets_table, entries)
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

    dispatch_role_removed(node, role)

    # Clean up node entry if no more roles remain for this node in this scope
    case Ets.lookup(@ets_table, {:scope, scope, :node, node}) do
      [] -> Ets.delete(@ets_nodes_table, {:scope, scope, node})
      _ -> :ok
    end
  end

  defp remove_node_entry(node, scope) do
    # Read current roles before removing to fire on_role_removed callbacks
    current_roles =
      case Ets.lookup(@ets_table, {:scope, scope, :node, node}) do
        [] -> []
        tuples -> for {_, role} <- tuples, do: role
      end

    Ets.match_delete(@ets_table, {{:scope, scope, :node, node}, :_})
    Ets.match_delete(@ets_table, {{:scope, scope, :role, :_}, node})
    Ets.delete(@ets_nodes_table, {:scope, scope, node})

    # Fire callbacks for each removed role
    Enum.each(current_roles, &dispatch_role_removed(node, &1))
  end

  defp broadcast(scope, message) do
    # :pg.broadcast/3 or /4 is not available in all OTP versions,
    # so we use :pg.get_members + send for efficient O(n) delivery.
    # Filtering out self() avoids processing our own broadcasts.
    local_pid = self()

    scope
    |> Pg.get_members(:all_nodes)
    |> Enum.each(fn
      pid when pid != local_pid -> send(pid, message)
      _ -> :ok
    end)
  end

  defp async_pull_node(scope, remote_node, msg_tag) do
    server = self()

    Task.Supervisor.start_child(@task_supervisor, fn ->
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

    Task.Supervisor.start_child(@task_supervisor, fn ->
      pull_roles_from_cluster(scope)
      send(server, {:pull_complete, scope})
    end)
  end

  # Generation-aware periodic pull: only do full RPC when generation has changed.
  defp async_pull_roles_from_cluster_with_generation_check(scope, state) do
    server = self()
    current_node = Node.self()
    live_nodes = Enum.reject(Node.list(), &(&1 == current_node))
    remote_gens = Map.get(state.remote_generations, scope, %{})

    Task.Supervisor.start_child(@task_supervisor, fn ->
      # Check generations and only pull if changed
      Enum.each(live_nodes, fn node ->
        known_gen = Map.get(remote_gens, node)

        case get_remote_generation(node, scope) do
          {:ok, remote_gen} when remote_gen != known_gen ->
            # Generation changed or node is new, do full pull
            case pull_roles_from_node(scope, node) do
              {:ok, roles} ->
                send(server, {:pull_update_node, scope, node, roles, remote_gen})

              {:error, reason} ->
                Logger.warning(
                  "ClusterHelper: failed to pull from #{inspect(node)}: #{inspect(reason)}"
                )
            end

          {:ok, _same_gen} ->
            # Generation unchanged, skip pull
            :ok

          {:error, reason} ->
            Logger.warning(
              "ClusterHelper: failed to get generation from #{inspect(node)}: #{inspect(reason)}"
            )

            # Fall back to full pull on generation check failure
            case pull_roles_from_node(scope, node) do
              {:ok, roles} ->
                send(server, {:pull_update_node, scope, node, roles})

              {:error, _} ->
                :ok
            end
        end
      end)

      # Clean up stale nodes (in ETS but not in Node.list())
      known_nodes = all_nodes(scope)
      stale_nodes = Enum.reject(known_nodes, &(&1 == current_node or &1 in live_nodes))

      Enum.each(stale_nodes, fn node ->
        send(server, {:stale_node, scope, node})
      end)
    end)
  end

  defp get_remote_generation(node, scope) do
    try do
      gen = :erpc.call(node, ClusterHelper.NodeConfig, :__get_generation__, [scope], 1000)
      {:ok, gen}
    catch
      :exit, reason -> {:error, reason}
    end
  end

  defp pull_roles_from_cluster(scope) do
    current_node = Node.self()

    live_nodes =
      Node.list()
      |> Enum.reject(&(&1 == current_node))

    Enum.each(live_nodes, &async_pull_node(scope, &1, :pull_update_node))

    stale_nodes = all_nodes(scope) -- [current_node | live_nodes]

    Enum.each(stale_nodes, fn node ->
      Logger.info(
        "ClusterHelper: node #{inspect(node)} left scope #{inspect(scope)}, removing roles"
      )

      remove_node_entry(node, scope)
    end)
  end

  defp pull_roles_from_node(scope, node) do
    timeout = Application.get_env(:cluster_helper, :pull_timeout, @default_timeout)
    do_pull_with_retry(scope, node, timeout, 3)
  end

  # Retry :erpc.call up to 3 times with 200ms sleep to handle :noproc during
  # peer startup. The periodic pull will catch any remaining failures.
  defp do_pull_with_retry(scope, node, timeout, retries) do
    retry_timeout = if retries < 3, do: min(timeout, 1000), else: timeout

    result =
      try do
        :erpc.call(node, ClusterHelper, :get_my_roles, [scope], retry_timeout)
      catch
        :exit, reason -> {:error, reason}
      end

    case result do
      {:error, {:exception, {:noproc, _}}} when retries > 1 ->
        Process.sleep(200)
        do_pull_with_retry(scope, node, timeout, retries - 1)

      {:error, {:noproc, _}} when retries > 1 ->
        Process.sleep(200)
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

  # ── Event dispatch helpers ───────────────────────────────────────────────────
  # These handle the new on_role_removed and on_node_removed callbacks.
  # They mirror the dispatch pattern in ClusterHelper.EventHandler for
  # consistency. The callbacks are defined as optional in the EventHandler
  # behaviour, so we check with function_exported? before calling.

  defp dispatch_role_removed(node, role) do
    with {:ok, mod} <- event_handler_config(),
         true <- function_exported?(mod, :on_role_removed, 2) do
      mod.on_role_removed(node, role)
    end

    :ok
  end

  defp dispatch_node_removed(node) do
    with {:ok, mod} <- event_handler_config(),
         true <- function_exported?(mod, :on_node_removed, 1) do
      mod.on_node_removed(node)
    end

    :ok
  end

  defp event_handler_config do
    case Application.get_env(:cluster_helper, :event_handler) do
      nil -> :no_handler
      mod -> {:ok, mod}
    end
  end
end
