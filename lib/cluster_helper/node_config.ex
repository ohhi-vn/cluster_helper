defmodule ClusterHelper.NodeConfig do
  use GenServer, restart: :transient

  @moduledoc """
  A module helper for node config.
  Support work with node config.
  """
  @ets_table __MODULE__

  alias :ets, as: Ets
  alias :syn, as: Syn

  @interval 7_000

  require Logger

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def add_role(role) do
    GenServer.cast(__MODULE__, {:add_roles, [role]})
  end

  def add_roles(roles) when is_list(roles) do
    GenServer.cast(__MODULE__, {:add_roles, roles})
  end

  def get_my_roles do
    GenServer.call(__MODULE__, :get_my_roles)
  end

  def get_nodes(role) do
    Ets.lookup(@ets_table, {:role, role})
    |> Enum.map(fn {_, node} -> node
    end)
  end

  def remote_get_roles(pid) do
    GenServer.call(pid, :get_my_roles)
  end

  def get_all_nodes do
    Ets.match_object(@ets_table, {{:role, :_}, :_})
    |> Enum.map(fn {_, node} -> node
    end)
    |> Enum.uniq()
  end

  def get_roles(node) do
    Ets.lookup(@ets_table, {:node, node})
    |> Enum.map(fn {_, role} -> role
    end)
  end

  def remove_role(role) do
    GenServer.cast(__MODULE__, {:remove_roles, [role]})
  end

  def remove_roles(roles) when is_list(roles) do
    GenServer.cast(__MODULE__, {:remove_roles, roles})
  end

  ## Callbacks ##

  @impl true
  def init (_) do
    Ets.new(@ets_table, [:bag, :named_table, read_concurrency: true, write_concurrency: true])

    {:ok, [], {:continue, :read_config}}
  end

  @impl true
  def handle_continue(:read_config, _state) do
    Syn.add_node_to_scopes([get_syn_scope()])

    roles = Application.get_env(:cluster_helper, :roles, [])

    if !is_list(roles) do
      Logger.error "Invalid config format (roles is a list). Please check your config file. Roles: #{inspect roles}"
    else
      Logger.debug("ClusterHelper, NodeConfig, node's roles: #{inspect(roles)}")

      do_add_my_roles(roles)
    end

    pull_roles()

    Syn.join(get_syn_scope(), :all_nodes, self(), Node.self())

    Syn.publish(get_syn_scope(), :all_nodes, {:new_node, Node.self(), self()})

    # TO-DO: Improve this, currently, this way limited by cannot detected new node join in cluster.
    Process.send_after(self(), :pull_roles, @interval)

    {:noreply, roles}
  end

  @impl true
  def handle_cast({:add_roles, roles}, state)  do
    Logger.debug("ClusterHelper, NodeConfig, add roles: #{inspect(roles)}, for current node #{inspect(Node.self())}")
    do_add_my_roles(roles)

    Logger.debug("ClusterHelper, NodeConfig, publish new roles: #{inspect(roles)}")
    Syn.publish(get_syn_scope(), :all_nodes, {:new_roles, roles, Node.self()})

    state = Enum.uniq(state ++ roles)

    {:noreply, state}
  end

  def handle_cast({:remove_roles, roles}, state) do
    node = Node.self()
    Logger.debug("ClusterHelper, NodeConfig, remove roles: #{inspect(roles)}, for current node #{inspect(node)}")
    Enum.each(roles, fn role ->
      remove_role(node, role)
    end)

    Syn.publish(get_syn_scope(), :all_nodes, {:remove_roles, roles, node})

    state = state -- roles

    {:noreply, state}
  end

  @impl true
  def handle_call(:get_my_roles, _from, roles) do
    {:reply, roles, roles}
  end

  @impl true
  def handle_info(:pull_roles, state) do
    pull_roles()
    Process.send_after(self(), :pull_roles, @interval)
    {:noreply, state}
  end

  def handle_info({:new_roles, roles, node}, state) do
    if node != Node.self() do
      Logger.debug("ClusterHelper, NodeConfig, new roles #{inspect(roles)} from #{inspect(node)}")
      do_add_roles(node, roles)
    end

    {:noreply, state}
  end

  def handle_info({:remove_roles, roles, node}, state) do
    if node != Node.self() do
      Logger.debug("ClusterHelper, NodeConfig, remove roles from #{inspect(node)}")
      Enum.each(roles, fn role ->
        remove_role(node, role)
      end)
    end

    {:noreply, state}
  end

  def handle_info({:new_node, node, pid}, state) do
    if node != Node.self() do
      Logger.debug("ClusterHelper, NodeConfig, pull roles from #{inspect(pid)} on #{inspect(node)}")
      roles = GenServer.call(pid, :get_my_roles)
      do_add_role(node, roles)
    end
    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.debug("ClusterHelper, NodeConfig, handle_info, unknown msg: #{inspect(msg)}")
    {:noreply, state}
  end


  ## Private functions ##

  defp do_add_role(node, role) do
    Ets.insert(@ets_table, {{:role, role}, node})
    Ets.insert(@ets_table, {{:node, node}, role})
  end

  defp do_add_my_role(role) do
    node = Node.self()
    Syn.join(get_syn_scope(), role, self(), node)
    do_add_role(node, role)
  end

  defp do_add_roles(node, roles) do
    Logger.debug("ClusterHelper, NodeConfig, add roles: #{inspect(roles)}, for node #{inspect(node)}")

    Enum.each(roles, fn role ->
      do_add_role(node, role)
    end)
  end

  defp do_add_my_roles(roles) do
    Logger.debug("ClusterHelper, NodeConfig, add roles: #{inspect(roles)}, for current node #{inspect(Node.self())}")

    Enum.each(roles, fn role ->
      do_add_my_role(role)
    end)
  end

  defp pull_roles do
    nodes = Syn.members(get_syn_scope(), :all_nodes)
    Enum.each(nodes, fn {pid, node} ->
      if node != Node.self() do
        Logger.debug("ClusterHelper, NodeConfig, pull roles from #{inspect(pid)} on #{inspect(node)}")
        roles = GenServer.call(pid, :get_my_roles)
        # Remove all old roles of node
        remove_node(node)
        # Add all roles of node
        do_add_roles(node, roles)
      end
    end)

    live_nodes = Enum.map(nodes, fn {_, node} -> node end)
    removed_nodes = get_all_nodes() -- live_nodes
    Enum.each(removed_nodes, fn node ->
      Logger.debug("ClusterHelper, NodeConfig, remove roles of #{inspect(node)}")
      remove_node(node)
    end)
  end

  defp remove_node(node) do
    Ets.delete(@ets_table, {:node, node})
    Ets.match_delete(@ets_table, {{:role, :_}, node})
  end

  defp remove_role(node, role) do
    Ets.match_delete(@ets_table, {{:role, role}, node})
    Ets.match_delete(@ets_table, {{:node, node}, role})
  end

  defp get_syn_scope do
    Application.get_env(:cluster_helper, :scope, ClusterHelper)
  end
end
