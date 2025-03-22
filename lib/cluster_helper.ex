defmodule ClusterHelper do
  @moduledoc """
  A module helper for cluster.
  Support work with dynamic cluster.
  Base on role and node.
  Each node can have a or many roles.
  And each role can have a or many nodes.
  """

  alias ClusterHelper.NodeConfig

  @doc """
  Return nodes have role.

  ## Examples

      iex> ClusterHelper.get_nodes(:data)
      [:node1, :node2]

  """
  def get_nodes(role) do
    NodeConfig.get_nodes(role)
  end

  @doc """
  Return roles of a node.

  ## Examples

      iex> ClusterHelper.get_roles(:node1)
      [:data, :web]

  """
  def get_roles(node) do
    NodeConfig.get_roles(node)
  end

  def all_nodes do
    NodeConfig.get_all_nodes()
  end

  def add_role(role) do
    NodeConfig.add_role(role)
  end

  def add_roles(roles) do
    NodeConfig.add_roles(roles)
  end

  def get_my_roles do
    NodeConfig.get_my_roles()
  end
end
