defmodule ClusterHelper.NodeConfig do
  use GenServer, restart: :transient

  @moduledoc """
  Manages node configuration and role assignments in a distributed cluster.

  This GenServer maintains an ETS table for fast role/node lookups and uses
  the :syn library for cluster-wide synchronization and pub/sub.
  """

  @ets_table __MODULE__

  alias :ets, as: Ets
  alias :syn, as: Syn

  @default_interval 7_000
  @default_timeout 5_000

  require Logger

  # Client API

  @spec start_link(any()) :: GenServer.on_start()
  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @spec add_role(atom()) :: :ok
  def add_role(role) when is_atom(role) do
    GenServer.cast(__MODULE__, {:add_roles, [role]})
  end

  @spec add_roles([atom()]) :: :ok
  def add_roles(roles) when is_list(roles) do
    GenServer.cast(__MODULE__, {:add_roles, roles})
  end

  @spec add_node(atom()) :: :ok
  def add_node(node) do
    GenServer.cast(__MODULE__, {:new_node, node, self()})
  end

  @spec remove_node(atom()) :: :ok
  def remove_node(node) do
    GenServer.cast(__MODULE__, {:remove_node, node, self()})
  end

  @spec get_my_roles() :: [atom()]
  def get_my_roles do
    GenServer.call(__MODULE__, :get_my_roles)
  end

  @spec get_nodes(atom()) :: [node()]
  def get_nodes(role) do
    Ets.lookup(@ets_table, {:role, role})
    |> Enum.map(fn {_, node} -> node end)
    |> Enum.uniq()
  end

  @spec get_all_nodes() :: [node()]
  def get_all_nodes do
    Ets.select(@ets_table, [
      {{{:role, :_}, :"$1"}, [], [:"$1"]}
    ])
    |> Enum.uniq()
  end

  @spec get_roles(node()) :: [atom()]
  def get_roles(node) do
    Ets.lookup(@ets_table, {:node, node})
    |> Enum.map(fn {_, role} -> role end)
  end

  @spec remove_role(atom()) :: :ok
  def remove_role(role) when is_atom(role) do
    GenServer.cast(__MODULE__, {:remove_roles, [role]})
  end

  @spec remove_roles([atom()]) :: :ok
  def remove_roles(roles) when is_list(roles) do
    GenServer.cast(__MODULE__, {:remove_roles, roles})
  end

  @spec local_node?(atom()) :: true | false
  def local_node?(node) do
    node == Node.self()
  end

  # Server Callbacks

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
    # Add node to syn scope
    Syn.add_node_to_scopes([get_syn_scope()])

    # Read and validate roles from config
    roles = Application.get_env(:cluster_helper, :roles, [])
    interval = Application.get_env(:cluster_helper, :pull_interval, @default_interval)

    roles =
      if is_list(roles) do
        Logger.info("ClusterHelper starting with roles: #{inspect(roles)}")
        do_add_my_roles(roles)
        roles
      else
        Logger.error("Invalid config format - roles must be a list. Got: #{inspect(roles)}")
        []
      end

    # Initial pull from existing nodes
    pull_roles()

    # Join the all_nodes group
    Syn.join(get_syn_scope(), :all_nodes, self(), Node.self())

    # Announce this node to the cluster
    Syn.publish(get_syn_scope(), :all_nodes, {:new_node, Node.self(), self()})

    # Schedule periodic pull
    schedule_pull(interval)

    # set for active pull roles from new node.
    Syn.set_event_handler(ClusterHelper.SynEventHandler)

    {:noreply, %{state | roles: roles}}
  end

  @impl true
  def handle_cast({:add_roles, new_roles}, state) do
    current_node = Node.self()

    # Filter out roles that already exist
    roles_to_add = new_roles -- state.roles

    if roles_to_add != [] do
      Logger.debug("Adding roles #{inspect(roles_to_add)} to node #{inspect(current_node)}")

      do_add_my_roles(roles_to_add)

      # Publish only new roles
      Syn.publish(get_syn_scope(), :all_nodes, {:new_roles, roles_to_add, current_node})

      {:noreply, %{state | roles: Enum.uniq(state.roles ++ roles_to_add)}}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:remove_roles, roles_to_remove}, state) do
    current_node = Node.self()

    Logger.debug("Removing roles #{inspect(roles_to_remove)} from node #{inspect(current_node)}")

    Enum.each(roles_to_remove, fn role ->
      do_remove_role(current_node, role)
    end)

    Syn.publish(get_syn_scope(), :all_nodes, {:remove_roles, roles_to_remove, current_node})

    {:noreply, %{state | roles: state.roles -- roles_to_remove}}
  end

  def handle_cast({:new_node, remote_node, _pid}, state) do
    # Spawn async process to pull roles from new node
    Task.start(fn ->
      Logger.debug("Pulling roles from new node: #{inspect(remote_node)}")

      case pull_roles_from_node(remote_node) do
        {:ok, roles} ->
          send(__MODULE__, {:pull_new_node, remote_node, roles})

        {:error, reason} ->
          Logger.warning("Failed to pull roles from #{inspect(remote_node)}: #{inspect(reason)}")
      end
    end)

    {:noreply, state}
  end

  def handle_cast({:remove_node, node, _pid}, state) do
    do_remove_node(node)

    {:noreply, state}
  end

  @impl true
  def handle_call(:get_my_roles, _from, state) do
    {:reply, state.roles, state}
  end

  @impl true
  def handle_info(:pull_roles, state) do
    pull_roles()

    interval = Application.get_env(:cluster_helper, :pull_interval, @default_interval)
    schedule_pull(interval)

    {:noreply, state}
  end

  def handle_info({:new_roles, roles, remote_node}, state) do
    if remote_node != Node.self() do
      Logger.debug("Received new roles #{inspect(roles)} from #{inspect(remote_node)}")
      do_add_roles(remote_node, roles)
    end

    {:noreply, state}
  end

  def handle_info({:remove_roles, roles, remote_node}, state) do
    if remote_node != Node.self() do
      Logger.debug("Removing roles #{inspect(roles)} from #{inspect(remote_node)}")

      Enum.each(roles, fn role ->
        do_remove_role(remote_node, role)
      end)
    end

    {:noreply, state}
  end

  def handle_info({:new_node, remote_node, _pid}, state) do
    if remote_node != Node.self() do
      # Spawn async process to pull roles from new node
      Task.start(fn ->
        Logger.debug("Pulling roles from new node: #{inspect(remote_node)}")

        case pull_roles_from_node(remote_node) do
          {:ok, roles} ->
            send(__MODULE__, {:pull_new_node, remote_node, roles})

          {:error, reason} ->
            Logger.warning(
              "Failed to pull roles from #{inspect(remote_node)}: #{inspect(reason)}"
            )
        end
      end)
    end

    {:noreply, state}
  end

  def handle_info({:pull_update_node, remote_node, roles}, state) do
    # Remove all old roles and add new ones (full sync)
    do_remove_node(remote_node)
    do_add_roles(remote_node, roles)

    {:noreply, state}
  end

  def handle_info({:pull_new_node, remote_node, roles}, state) do
    # Add all roles from newly discovered node
    do_add_roles(remote_node, roles)

    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.debug("Received unknown message: #{inspect(msg)}")
    {:noreply, state}
  end

  # Private Functions

  defp do_add_role(node, role) do
    Ets.insert(@ets_table, {{:role, role}, node})
    Ets.insert(@ets_table, {{:node, node}, role})
  end

  defp do_add_my_role(role) do
    current_node = Node.self()
    Syn.join(get_syn_scope(), role, self(), current_node)
    do_add_role(current_node, role)
  end

  defp do_add_roles(node, roles) do
    Enum.each(roles, fn role ->
      do_add_role(node, role)
    end)
  end

  defp do_add_my_roles(roles) do
    Enum.each(roles, fn role ->
      do_add_my_role(role)
    end)
  end

  defp pull_roles do
    members = Syn.members(get_syn_scope(), :all_nodes)
    current_node = Node.self()

    # Get live nodes from syn
    live_nodes =
      members
      |> Enum.map(fn {_pid, node} -> node end)
      |> Enum.reject(&(&1 == current_node))

    # Spawn async task to pull from all nodes
    Task.start(fn ->
      Enum.each(live_nodes, fn remote_node ->
        case pull_roles_from_node(remote_node) do
          {:ok, roles} ->
            send(__MODULE__, {:pull_update_node, remote_node, roles})

          {:error, reason} ->
            Logger.debug("Failed to pull roles from #{inspect(remote_node)}: #{inspect(reason)}")
        end
      end)
    end)

    # Clean up roles from nodes that left the cluster
    current_nodes_in_ets = get_all_nodes()
    removed_nodes = current_nodes_in_ets -- (live_nodes ++ [current_node])

    Enum.each(removed_nodes, fn node ->
      Logger.info("Node #{inspect(node)} left the cluster, removing its roles")
      do_remove_node(node)
    end)
  end

  defp pull_roles_from_node(node) do
    timeout = Application.get_env(:cluster_helper, :pull_timeout, @default_timeout)

    try do
      roles = :erpc.call(node, ClusterHelper, :get_my_roles, [], timeout)
      {:ok, roles}
    rescue
      error ->
        {:error, error}
    catch
      :exit, reason ->
        {:error, reason}
    end
  end

  defp do_remove_node(node) do
    # Get all roles for the node before deleting
    roles = get_roles(node)

    # Delete from ETS
    Ets.delete(@ets_table, {:node, node})

    # Delete all role entries for this node
    Enum.each(roles, fn role ->
      Ets.delete_object(@ets_table, {{:role, role}, node})
    end)
  end

  defp do_remove_role(node, role) do
    Ets.delete_object(@ets_table, {{:role, role}, node})
    Ets.delete_object(@ets_table, {{:node, node}, role})
  end

  defp schedule_pull(interval) do
    Process.send_after(self(), :pull_roles, interval)
  end

  defp get_syn_scope do
    Application.get_env(:cluster_helper, :scope, ClusterHelper)
  end
end
