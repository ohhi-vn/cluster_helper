defmodule ClusterHelper.NodeConfig do
  @moduledoc """
  GenServer that owns the ETS role/node table and coordinates cluster sync.

  ## Responsibilities

  - Maintains an ETS `:bag` table keyed on `{:role, role}` and `{:node, node}`
    for O(1) lookups in either direction. Roles may be any Elixir term.
  - Registers this node in a `:syn` group so the `SynEventHandler` is notified
    of remote node arrivals/departures.
  - On startup, pulls the current role state from every already-connected node.
  - Publishes incremental add/remove events so remote nodes stay up-to-date
    without waiting for the next periodic pull.
  - Periodically re-syncs to recover from lost messages or transient failures.
  - Invokes the optional `ClusterHelper.EventHandler` callbacks when roles or
    nodes are first added to the local ETS table.

  ## Internal message protocol

  | Message | Direction | Meaning |
  |---------|-----------|---------|
  | `{:new_roles, roles, node}` | pub/sub → handle_info | Remote node added roles |
  | `{:remove_roles, roles, node}` | pub/sub → handle_info | Remote node removed roles |
  | `{:new_node, node, pid}` | pub/sub → handle_info | Remote node came online |
  | `{:pull_new_node, node, roles}` | internal Task → handle_info | Pulled roles from a newly discovered node |
  | `{:pull_update_node, node, roles}` | internal Task → handle_info | Full-sync result for a known node |
  | `:pull_roles` | Process.send_after → handle_info | Periodic sync tick |
  """

  use GenServer, restart: :transient

  require Logger

  alias :ets, as: Ets
  alias :syn, as: Syn
  alias ClusterHelper.EventHandler

  @ets_table __MODULE__
  @default_interval 7_000
  @default_timeout 5_000

  # ── Client API ────────────────────────────────────────────────────────────────

  @spec start_link(any()) :: GenServer.on_start()
  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  @spec get_my_roles() :: [ClusterHelper.role()]
  def get_my_roles(), do: GenServer.call(__MODULE__, :get_my_roles)

  @spec get_nodes(ClusterHelper.role()) :: [node()]
  def get_nodes(role) do
    @ets_table
    |> Ets.lookup({:role, role})
    |> Enum.map(fn {_key, node} -> node end)
  end

  @spec get_all_nodes() :: [node()]
  def get_all_nodes do
    Ets.select(@ets_table, [{{{:role, :_}, :"$1"}, [], [:"$1"]}])
    |> Enum.uniq()
  end

  @spec get_roles(node()) :: [ClusterHelper.role()]
  def get_roles(node) do
    @ets_table
    |> Ets.lookup({:node, node})
    |> Enum.map(fn {_key, role} -> role end)
  end

  # Guards are removed — roles can be any Elixir term.
  @spec add_role(ClusterHelper.role()) :: :ok
  def add_role(role), do: GenServer.call(__MODULE__, {:add_roles, [role]})

  @spec add_roles([ClusterHelper.role()]) :: :ok
  def add_roles(roles) when is_list(roles), do: GenServer.call(__MODULE__, {:add_roles, roles})

  @spec remove_role(ClusterHelper.role()) :: :ok
  def remove_role(role), do: GenServer.call(__MODULE__, {:remove_roles, [role]})

  @spec remove_roles([ClusterHelper.role()]) :: :ok
  def remove_roles(roles) when is_list(roles), do: GenServer.call(__MODULE__, {:remove_roles, roles})

  # These remain casts — they are triggered by external events (syn callbacks)
  # and the caller does not need confirmation.
  @spec add_node(atom()) :: :ok
  def add_node(node), do: GenServer.cast(__MODULE__, {:pull_from_node, node})

  @spec remove_node(atom()) :: :ok
  def remove_node(node), do: GenServer.cast(__MODULE__, {:remove_node, node})

  @spec local_node?(atom()) :: boolean()
  def local_node?(node), do: node == Node.self()

  # ── Server callbacks ──────────────────────────────────────────────────────────

  @impl true
  def init(_) do
    Ets.new(@ets_table, [
      :bag,
      :named_table,
      :protected,
      read_concurrency: true,
      write_concurrency: :auto
    ])

    {:ok, %{roles: []}, {:continue, :read_config}}
  end

  @impl true
  def handle_continue(:read_config, state) do
    scope = syn_scope()
    Syn.add_node_to_scopes([scope])

    roles =
      case Application.get_env(:cluster_helper, :roles, []) do
        roles when is_list(roles) ->
          Logger.info("ClusterHelper starting with roles: #{inspect(roles)}")
          add_my_roles(roles)
          roles

        bad ->
          Logger.error("ClusterHelper: :roles must be a list, got: #{inspect(bad)}")
          []
      end

    # Initial pull from any nodes already in the cluster.
    pull_roles_from_cluster()

    # Announce ourselves and join the all_nodes group.
    Syn.join(scope, :all_nodes, self(), Node.self())
    Syn.publish(scope, :all_nodes, {:new_node, Node.self(), self()})

    # Set the event handler for scope node-up/down events.
    Syn.set_event_handler(ClusterHelper.SynEventHandler)

    interval = Application.get_env(:cluster_helper, :pull_interval, @default_interval)
    schedule_pull(interval)

    {:noreply, %{state | roles: roles}}
  end

  # ── Calls ─────────────────────────────────────────────────────────────────────

  @impl true
  def handle_call({:add_roles, new_roles}, _from, state) do
    roles_to_add = new_roles -- state.roles

    if roles_to_add != [] do
      Logger.debug("Adding roles #{inspect(roles_to_add)} to #{inspect(Node.self())}")
      add_my_roles(roles_to_add)
      Syn.publish(syn_scope(), :all_nodes, {:new_roles, roles_to_add, Node.self()})
      {:reply, :ok, %{state | roles: Enum.uniq(state.roles ++ roles_to_add)}}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call({:remove_roles, roles_to_remove}, _from, state) do
    Logger.debug("Removing roles #{inspect(roles_to_remove)} from #{inspect(Node.self())}")
    Enum.each(roles_to_remove, &remove_role_entry(Node.self(), &1))
    Syn.publish(syn_scope(), :all_nodes, {:remove_roles, roles_to_remove, Node.self()})
    {:reply, :ok, %{state | roles: state.roles -- roles_to_remove}}
  end

  def handle_call(:get_my_roles, _from, state), do: {:reply, state.roles, state}

  # ── Casts ─────────────────────────────────────────────────────────────────────

  # Triggered by SynEventHandler when a remote scope node comes up.
  @impl true
  def handle_cast({:pull_from_node, remote_node}, state) do
    async_pull_node(remote_node, :pull_new_node)
    {:noreply, state}
  end

  def handle_cast({:remove_node, node}, state) do
    remove_node_entry(node)
    {:noreply, state}
  end

  # ── Infos ─────────────────────────────────────────────────────────────────────

  @impl true
  # Periodic sync tick.
  def handle_info(:pull_roles, state) do
    pull_roles_from_cluster()
    schedule_pull(Application.get_env(:cluster_helper, :pull_interval, @default_interval))
    {:noreply, state}
  end

  # A remote node published that it came online — pull its roles.
  def handle_info({:new_node, remote_node, _pid}, state) do
    if remote_node != Node.self() do
      async_pull_node(remote_node, :pull_new_node)
    end

    {:noreply, state}
  end

  # Remote node added roles — update ETS directly (no extra RPC needed).
  def handle_info({:new_roles, roles, remote_node}, state) do
    if remote_node != Node.self() do
      Logger.debug("Received new roles #{inspect(roles)} from #{inspect(remote_node)}")
      add_roles_for_node(remote_node, roles)
    end

    {:noreply, state}
  end

  # Remote node removed roles — evict from ETS.
  def handle_info({:remove_roles, roles, remote_node}, state) do
    if remote_node != Node.self() do
      Logger.debug("Removing roles #{inspect(roles)} from #{inspect(remote_node)}")
      Enum.each(roles, &remove_role_entry(remote_node, &1))
    end

    {:noreply, state}
  end

  # Full-sync result: replace stale data for a known node (periodic pull).
  # No on_node_added callback here — the node was already known.
  def handle_info({:pull_update_node, remote_node, roles}, state) do
    remove_node_entry(remote_node)
    add_roles_for_node(remote_node, roles)
    {:noreply, state}
  end

  # First-time pull for a newly discovered node — fire on_node_added callback.
  def handle_info({:pull_new_node, remote_node, roles}, state) do
    EventHandler.dispatch_node_added(remote_node)
    add_roles_for_node(remote_node, roles)
    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.debug("ClusterHelper.NodeConfig received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  # ── Private helpers ───────────────────────────────────────────────────────────

  # Add a single role for any node into ETS, then fire the callback.
  defp add_role_entry(node, role) do
    Ets.insert(@ets_table, {{:role, role}, node})
    Ets.insert(@ets_table, {{:node, node}, role})
    EventHandler.dispatch_role_added(node, role)
  end

  # Add a single role for the *local* node (also joins the syn group).
  defp add_my_role(role) do
    Syn.join(syn_scope(), role, self(), Node.self())
    add_role_entry(Node.self(), role)
  end

  defp add_my_roles(roles), do: Enum.each(roles, &add_my_role/1)

  defp add_roles_for_node(node, roles), do: Enum.each(roles, &add_role_entry(node, &1))

  # Remove a specific role for a node from ETS.
  defp remove_role_entry(node, role) do
    Ets.delete_object(@ets_table, {{:role, role}, node})
    Ets.delete_object(@ets_table, {{:node, node}, role})
  end

  # Remove all role entries for a node from ETS.
  defp remove_node_entry(node) do
    roles = get_roles(node)
    Ets.delete(@ets_table, {:node, node})
    Enum.each(roles, fn role ->
      Ets.delete_object(@ets_table, {{:role, role}, node})
    end)
  end

  # Spawn a Task to pull roles from `remote_node` and deliver the result
  # back to this GenServer as `{msg_tag, remote_node, roles}`.
  defp async_pull_node(remote_node, msg_tag) do
    server = self()

    Task.start(fn ->
      Logger.debug("Pulling roles from #{inspect(remote_node)}")

      case pull_roles_from_node(remote_node) do
        {:ok, roles} ->
          send(server, {msg_tag, remote_node, roles})

        {:error, reason} ->
          Logger.warning(
            "ClusterHelper: failed to pull from #{inspect(remote_node)}: #{inspect(reason)}"
          )
      end
    end)
  end

  # Perform a full sync against every live node that `:syn` knows about.
  defp pull_roles_from_cluster do
    current_node = Node.self()

    live_nodes =
      syn_scope()
      |> Syn.members(:all_nodes)
      |> Enum.map(fn {_pid, node} -> node end)
      |> Enum.reject(&(&1 == current_node))

    Enum.each(live_nodes, &async_pull_node(&1, :pull_update_node))

    stale_nodes = get_all_nodes() -- [current_node | live_nodes]

    Enum.each(stale_nodes, fn node ->
      Logger.info("ClusterHelper: node #{inspect(node)} left the cluster, removing roles")
      remove_node_entry(node)
    end)
  end

  # RPC to retrieve the roles list from a remote node.
  defp pull_roles_from_node(node) do
    timeout = Application.get_env(:cluster_helper, :pull_timeout, @default_timeout)

    try do
      roles = :erpc.call(node, ClusterHelper, :get_my_roles, [], timeout)
      {:ok, roles}
    rescue
      error -> {:error, error}
    catch
      :exit, reason -> {:error, reason}
    end
  end

  defp schedule_pull(interval), do: Process.send_after(self(), :pull_roles, interval)

  defp syn_scope, do: Application.get_env(:cluster_helper, :scope, ClusterHelper)
end
