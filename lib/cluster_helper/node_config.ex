defmodule ClusterHelper.NodeConfig do
  @moduledoc """
  GenServer that orchestrates cluster role management.

  This is the **application / orchestration layer** in Clean Architecture terms.
  It coordinates:
    * `ClusterHelper.ETSStore` — persists role↔node mappings
    * `ClusterHelper.Broadcast` — cluster-wide event propagation via `:pg`
    * `ClusterHelper.PullEngine` — background sync with generation-based detection
    * `ClusterHelper.RemoteSync` — RPC to remote nodes
    * `ClusterHelper.EventHandler` — user-defined callbacks
    * `ClusterHelper.ClusterState` — pure in-memory state

  ## What changed (v0.7 → v0.8)

  Previously, `NodeConfig` was a ~1000-line God module mixing ETS, `:pg`,
  RPC, config, and business logic. It has been decomposed into focused
  modules — each with a single responsibility — while keeping the same
  GenServer client API for full backward compatibility.
  """

  use GenServer, restart: :transient

  require Logger

  alias ClusterHelper.ClusterState
  alias ClusterHelper.EventHandler
  alias ClusterHelper.Telemetry

  @task_supervisor ClusterHelper.TaskSupervisor

  # ── Client API ────────────────────────────────────────────────────────────────

  @spec start_link(any()) :: GenServer.on_start()
  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @doc "Returns all roles for the local node. Reads directly from ETS."
  @spec get_my_roles(atom() | nil) :: [term()]
  def get_my_roles(scope \\ nil) do
    role_store().get_my_roles(resolve_scope(scope))
  end

  @doc "Returns nodes with the given role. Reads directly from ETS."
  @spec get_nodes(term(), atom() | nil) :: [node()]
  def get_nodes(role, scope \\ nil) do
    role_store().get_nodes(role, resolve_scope(scope))
  end

  @doc "Returns roles for a node. Reads directly from ETS."
  @spec get_roles(node(), atom() | nil) :: [term()]
  def get_roles(node, scope \\ nil) do
    role_store().get_roles(node, resolve_scope(scope))
  end

  @doc "Returns all nodes with at least one role. Reads directly from ETS."
  @spec all_nodes(atom() | nil) :: [node()]
  def all_nodes(scope \\ nil) do
    role_store().all_nodes(resolve_scope(scope))
  end

  @doc "Adds a single role to the local node and propagates cluster-wide."
  @spec add_role(term(), atom() | nil) :: :ok
  def add_role(role, scope \\ nil),
    do: GenServer.call(__MODULE__, {:add_roles, resolve_scope(scope), [role]})

  @doc "Adds multiple roles to the local node and propagates cluster-wide."
  @spec add_roles([term()], atom() | nil) :: :ok
  def add_roles(roles, scope \\ nil) when is_list(roles),
    do: GenServer.call(__MODULE__, {:add_roles, resolve_scope(scope), roles})

  @doc "Removes a single role from the local node and propagates cluster-wide."
  @spec remove_role(term(), atom() | nil) :: :ok
  def remove_role(role, scope \\ nil),
    do: GenServer.call(__MODULE__, {:remove_roles, resolve_scope(scope), [role]})

  @doc "Removes multiple roles from the local node and propagates cluster-wide."
  @spec remove_roles([term()], atom() | nil) :: :ok
  def remove_roles(roles, scope \\ nil) when is_list(roles),
    do: GenServer.call(__MODULE__, {:remove_roles, resolve_scope(scope), roles})

  @doc "Joins an additional scope."
  @spec join_scope(atom()) :: :ok | {:error, :already_joined}
  def join_scope(scope) when is_atom(scope),
    do: GenServer.call(__MODULE__, {:join_scope, scope})

  @doc "Leaves a scope, removing all roles and cleaning up."
  @spec leave_scope(atom()) :: :ok | {:error, :not_joined}
  def leave_scope(scope) when is_atom(scope),
    do: GenServer.call(__MODULE__, {:leave_scope, scope})

  @doc "Lists all scopes the local node is participating in."
  @spec list_scopes() :: [atom()]
  def list_scopes do
    GenServer.call(__MODULE__, :list_scopes)
  end

  @doc "Returns true when the node is the local node."
  @spec local_node?(node()) :: boolean()
  def local_node?(node), do: node == Node.self()

  @doc false
  @spec __get_generation__(atom()) :: integer()
  def __get_generation__(scope) do
    GenServer.call(__MODULE__, {:__get_generation__, scope})
  end

  @doc false
  @spec __get_scopes__() :: [atom()]
  def __get_scopes__ do
    GenServer.call(__MODULE__, :__get_scopes__)
  end

  # ── Server callbacks ──────────────────────────────────────────────────────────

  @impl true
  def init(_) do
    role_store().init()
    ensure_task_supervisor()
    {:ok, ClusterState.new(), {:continue, :setup_cluster}}
  end

  @impl true
  def handle_continue(:setup_cluster, state) do
    :net_kernel.monitor_nodes(true, node_type: :visible)
    start_time = System.monotonic_time()

    config = ClusterHelper.Config.from_app_env()

    state =
      case configured_scopes(config) do
        {:ok, scopes} ->
          Enum.reduce(scopes, state, fn scope, acc ->
            ClusterHelper.Broadcast.start_scope(scope)
            ClusterHelper.Broadcast.join(scope)
            ClusterState.add_scope(acc, scope)
          end)

        {:error, scopes} ->
          Enum.reduce(scopes, state, fn scope, acc ->
            ClusterHelper.Broadcast.start_scope(scope)
            ClusterHelper.Broadcast.join(scope)
            ClusterState.add_scope(acc, scope)
          end)
      end

    default_scope = config.scope
    roles = config.roles

    state =
      if roles != [] do
        Logger.info(
          "ClusterHelper starting with roles: #{inspect(roles)} in scope #{inspect(default_scope)}"
        )

        role_store().insert_roles(Node.self(), default_scope, roles)
        {state, _added, gen} = ClusterState.add_roles(state, default_scope, roles)
        %{state | generations: Map.put(state.generations, default_scope, gen)}
      else
        state
      end

    Enum.each(state.scopes, fn scope ->
      ClusterHelper.PullEngine.pull_all(self(), scope)
    end)

    schedule_pull(config.pull_interval)

    duration = System.monotonic_time() - start_time
    Telemetry.emit_startup(duration, MapSet.to_list(state.scopes), roles)

    {:noreply, state}
  end

  # ── Calls ─────────────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:add_roles, scope, new_roles}, _from, state) do
    state = ensure_scope_in_state(state, scope)
    {state, roles_to_add, _gen} = ClusterState.add_roles(state, scope, new_roles)

    if roles_to_add != [] do
      Logger.debug(
        "Adding roles #{inspect(roles_to_add)} to #{inspect(Node.self())} in scope #{inspect(scope)}"
      )

      role_store().insert_roles(Node.self(), scope, roles_to_add)
      ClusterHelper.Broadcast.broadcast(scope, {:new_roles, scope, roles_to_add, Node.self()})
      Telemetry.emit_role_add(scope, roles_to_add, Node.self())
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:remove_roles, scope, roles_to_remove}, _from, state) do
    state = ensure_scope_in_state(state, scope)
    {state, actually_removed, _gen} = ClusterState.remove_roles(state, scope, roles_to_remove)

    if actually_removed != [] do
      Logger.debug(
        "Removing roles #{inspect(actually_removed)} from #{inspect(Node.self())} in scope #{inspect(scope)}"
      )

      Enum.each(actually_removed, &role_store().delete_role(Node.self(), scope, &1))

      ClusterHelper.Broadcast.broadcast(
        scope,
        {:remove_roles, scope, actually_removed, Node.self()}
      )

      Telemetry.emit_role_remove(scope, actually_removed, Node.self())
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:join_scope, scope}, _from, state) do
    if ClusterState.has_scope?(state, scope) do
      {:reply, {:error, :already_joined}, state}
    else
      ClusterHelper.Broadcast.start_scope(scope)
      ClusterHelper.Broadcast.join(scope)
      state = ClusterState.add_scope(state, scope)
      ClusterHelper.PullEngine.pull_all(self(), scope)
      {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:leave_scope, scope}, _from, state) do
    if ClusterState.has_scope?(state, scope) do
      current_roles = role_store().get_my_roles(scope)

      Enum.each(current_roles, &role_store().delete_role(Node.self(), scope, &1))

      role_store().purge_scope(scope)
      ClusterHelper.Broadcast.leave(scope)
      state = ClusterState.remove_scope(state, scope)
      {:reply, :ok, state}
    else
      {:reply, {:error, :not_joined}, state}
    end
  end

  @impl true
  def handle_call(:list_scopes, _from, state) do
    {:reply, MapSet.to_list(state.scopes), state}
  end

  @impl true
  def handle_call({:__get_generation__, scope}, _from, state) do
    {:reply, Map.get(state.generations, scope, 0), state}
  end

  @impl true
  def handle_call(:__get_scopes__, _from, state) do
    {:reply, MapSet.to_list(state.scopes), state}
  end

  # ── Infos ─────────────────────────────────────────────────────────────────────

  @impl true
  def handle_info(:pull_roles, state) do
    Enum.each(state.scopes, fn scope ->
      ClusterHelper.PullEngine.pull_all_with_generation_check(
        self(),
        scope,
        state.remote_generations
      )
    end)

    config = ClusterHelper.Config.from_app_env()
    schedule_pull(config.pull_interval)
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodeup, remote_node, _info}, state) do
    if remote_node != Node.self() do
      Logger.debug("Node up: #{inspect(remote_node)}")
      Telemetry.emit_node_up(remote_node, MapSet.to_list(state.scopes))
      ClusterHelper.PullEngine.pull_new_node(self(), remote_node, state.scopes)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, remote_node, _info}, state) do
    Logger.debug("Node down: #{inspect(remote_node)}")
    Telemetry.emit_node_down(remote_node)

    Enum.each(state.scopes, fn scope ->
      role_store().delete_node(remote_node, scope)
    end)

    EventHandler.dispatch_node_removed(remote_node)

    state =
      state
      |> ClusterState.remove_known_node_from_all(remote_node)
      |> ClusterState.remove_remote_gen_from_all(remote_node)

    {:noreply, state}
  end

  @impl true
  def handle_info({:new_roles, scope, roles, remote_node}, state) do
    if remote_node != Node.self() and ClusterState.has_scope?(state, scope) do
      Logger.debug(
        "Received new roles #{inspect(roles)} from #{inspect(remote_node)} in scope #{inspect(scope)}"
      )

      role_store().insert_roles(remote_node, scope, roles)
      state = ClusterState.add_known_node(state, scope, remote_node)
      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  @impl true
  def handle_info({:remove_roles, scope, roles, remote_node}, state) do
    if remote_node != Node.self() and ClusterState.has_scope?(state, scope) do
      Logger.debug(
        "Removing roles #{inspect(roles)} from #{inspect(remote_node)} in scope #{inspect(scope)}"
      )

      Enum.each(roles, &role_store().delete_role(remote_node, scope, &1))
    end

    {:noreply, state}
  end

  @impl true
  def handle_info({:pull_update_node, scope, remote_node, roles}, state) do
    do_handle_pull_update_node(scope, remote_node, roles, nil, state)
  end

  @impl true
  def handle_info({:pull_update_node, scope, remote_node, roles, new_gen}, state) do
    do_handle_pull_update_node(scope, remote_node, roles, new_gen, state)
  end

  @impl true
  def handle_info({:pull_new_node, scope, remote_node, roles}, state) do
    do_handle_pull_new_node(scope, remote_node, roles, nil, state)
  end

  @impl true
  def handle_info({:pull_new_node, scope, remote_node, roles, new_gen}, state) do
    do_handle_pull_new_node(scope, remote_node, roles, new_gen, state)
  end

  @impl true
  def handle_info({:stale_node, scope, node}, state) do
    Logger.info(
      "ClusterHelper: node #{inspect(node)} left scope #{inspect(scope)}, removing roles"
    )

    role_store().delete_node(node, scope)

    state =
      state
      |> ClusterState.remove_known_node(scope, node)
      |> then(fn s ->
        update_in(s, [:remote_generations, scope], &Map.delete(&1 || %{}, node))
      end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:pull_complete, _scope}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.debug("ClusterHelper.NodeConfig received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  # ── Private helpers ───────────────────────────────────────────────────────────

  defp do_handle_pull_update_node(scope, remote_node, roles, new_gen, state) do
    role_store().delete_node(remote_node, scope)
    role_store().insert_roles(remote_node, scope, roles)

    state =
      state
      |> ClusterState.add_known_node(scope, remote_node)

    state =
      if new_gen do
        ClusterState.put_remote_generation(state, scope, remote_node, new_gen)
      else
        state
      end

    {:noreply, state}
  end

  defp do_handle_pull_new_node(scope, remote_node, roles, new_gen, state) do
    EventHandler.dispatch_node_added(remote_node)
    role_store().insert_roles(remote_node, scope, roles)

    state =
      state
      |> ClusterState.add_known_node(scope, remote_node)

    state =
      if new_gen do
        ClusterState.put_remote_generation(state, scope, remote_node, new_gen)
      else
        state
      end

    {:noreply, state}
  end

  defp ensure_scope_in_state(state, scope) do
    if ClusterState.has_scope?(state, scope) do
      state
    else
      Logger.info("Auto-joining scope #{inspect(scope)}")
      ClusterHelper.Broadcast.start_scope(scope)
      ClusterHelper.Broadcast.join(scope)
      ClusterState.add_scope(state, scope)
    end
  end

  @doc false
  def ensure_task_supervisor do
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

  defp schedule_pull(interval), do: Process.send_after(self(), :pull_roles, interval)

  defp role_store, do: ClusterHelper.Adapter.for(:role_store)

  defp resolve_scope(nil), do: ClusterHelper.Config.from_app_env().scope
  defp resolve_scope(scope) when is_atom(scope), do: scope

  @doc false
  def configured_scopes(config) do
    case config.scopes do
      nil ->
        {:ok, [config.scope]}

      scopes when is_list(scopes) ->
        {:ok, scopes}

      bad ->
        Logger.error("ClusterHelper: :scopes must be a list, got: #{inspect(bad)}")
        {:error, [config.scope]}
    end
  end
end
