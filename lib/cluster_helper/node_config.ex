defmodule ClusterHelper.NodeConfig do
  use GenServer, restart: :transient

  @moduledoc """
  A module helper for node config.
  Support work with node config.
  """
  @ets_table __MODULE__

  alias :ets, as: Ets
  alias :syn, as: Syn

  @interval 5_000

  require Logger

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def add_role(role) do
    GenServer.cast(__MODULE__, {:add_role, role})
  end

  def add_roles(roles) do
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
      Logger.error "Invalid config format (roles is a list). Please check your config file."
    else
      Logger.debug("ClusterHelper, NodeConfig, node's roles: #{inspect(roles)}")

      do_add_roles(roles)
    end

    Syn.join(get_syn_scope(), :all_nodes, self(), Node.self())

    pull_roles()

    Syn.publish(get_syn_scope(), :all_nodes, {:new_node, Node.self(), self()})

    # TO-DO: Improve this, currently, this way limited by cannot detected new node join in cluster.
    Process.send_after(self(), :pull_roles, @interval)

    {:noreply, roles}
  end

  @impl true
  def handle_cast({:add_role, role}, state) do
    Logger.debug("ClusterHelper, NodeConfig, add role: #{inspect(role)}, for #{inspect(Node.self())}")

    do_add_role(role)

    state = Enum.uniq([role | state])

    {:noreply, state}
  end

  def handle_cast({:add_roles, roles}, state) do
    Logger.debug("ClusterHelper, NodeConfig, add roles: #{inspect(roles)}, for #{inspect(Node.self())}")
    do_add_roles(roles)

    state = Enum.uniq(state ++ roles)

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

  defp do_add_role(role) do
    Syn.join(get_syn_scope(), role, self(), Node.self())
    Ets.insert(@ets_table, {{:role, role}, Node.self()})
    Ets.insert(@ets_table, {{:node, Node.self()}, role})
  end

  defp do_add_role(node, roles) do
    Enum.each(roles, fn role ->
      Logger.debug("ClusterHelper, NodeConfig, add role: #{inspect(role)}, for #{inspect(node)}")
      Ets.insert(@ets_table, {{:role, role}, node})
      Ets.insert(@ets_table, {{:node, node}, role})
    end)
  end

  defp do_add_roles(roles) do
    Enum.each(roles, fn role ->
      Logger.debug("ClusterHelper, NodeConfig, add role: #{inspect(role)}, for #{inspect(Node.self())}")
      do_add_role(role)
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
        do_add_role(node, roles)
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

  defp get_syn_scope do
    Application.get_env(:cluster_helper, :scope, ClusterHelper)
  end
end
