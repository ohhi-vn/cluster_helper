defmodule ClusterHelper.NodeConfig do
  use GenServer, restart: :transient

  @moduledoc """
  A module helper for node config.
  Support work with node config.
  """
  @ets_table __MODULE__

  alias :ets, as: Ets
  alias :syn, as: Syn

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

    Syn.add_node_to_scopes([get_syn_scope()])

    {:ok, [], {:continue, :read_config}}
  end

  @impl true
  def handle_continue(:read_config, _state) do
    roles = Application.get_env(:cluster_helper, :roles, [])

    if !is_list(roles) do
      Logger.error "Invalid config format (roles is a list). Please check your config file."
    else
      Logger.debug("ClusterHelper, NodeConfig, node's roles: #{inspect(roles)}")

      do_add_roles(roles)
    end

    Syn.join(get_syn_scope(), :all_nodes, self(), Node.self())

    pull_roles()

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

  ## Private functions ##

  defp do_add_role(role) do
    Syn.join(get_syn_scope(), role, self(), Node.self())
    Ets.insert(@ets_table, {{:role, role}, Node.self()})
    Ets.insert(@ets_table, {{:node, Node.self()}, role})
  end

  defp do_add_roles(roles) do
    Enum.each(roles, fn role ->
      Logger.debug("ClusterHelper, NodeConfig, add role: #{inspect(role)}, for #{inspect(Node.self())}")
      do_add_role(role)
    end)
  end

  defp pull_roles do
    Syn.members(get_syn_scope(), "all_nodes")
    |> Enum.each(fn {pid, node} ->
      Logger.debug("ClusterHelper, NodeConfig, pull roles from #{inspect(pid)} on #{inspect(node)}")
      GenServer.call(pid, :get_my_roles)
      |> Enum.each(fn role ->
        Logger.debug("ClusterHelper, NodeConfig, add role: #{inspect(role)}, for #{inspect(node)}")
        Ets.insert(@ets_table, {{:role, role}, node})
        Ets.insert(@ets_table, {{:node, node}, role})
      end)
    end)
  end

  defp get_syn_scope do
    Application.get_env(:cluster_helper, :scope, ClusterHelper)
  end
end
