defmodule ClusterHelper do
  @moduledoc """
  A module helper for cluster.
  Support work with dynamic cluster.
  Base on role and node.
  Each node can have a or many roles.
  And each role can have a or many nodes.
  """

  alias ClusterHelper.NodeConfig

  @spec get_nodes(any()) :: list()
  @doc """
  Return nodes have role.
  """
  def get_nodes(role) do
    NodeConfig.get_nodes(role)
  end

  @doc """
  Return roles of a node.
  """
  def get_roles(node) do
    NodeConfig.get_roles(node)
  end

  @doc """
  Return all nodes in cluster.
  """
  def all_nodes do
    NodeConfig.get_all_nodes()
  end

  @doc """
  Add a role for current node.
  """
  def add_role(role) do
    NodeConfig.add_role(role)
  end

  @doc """
  Add roles for current node.
  """
  def add_roles(roles) do
    NodeConfig.add_roles(roles)
  end

  @doc """
  Return roles of current node.
  """
  def get_my_roles do
    NodeConfig.get_my_roles()
  end

  @doc """
  Remove a role of current node.
  """
  def remove_role(role) do
    NodeConfig.remove_role(role)
  end
end
