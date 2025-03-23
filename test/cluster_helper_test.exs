defmodule ClusterHelperTest do
  use ExUnit.Case, async: false

  doctest ClusterHelper

  setup do
    roles = ClusterHelper.get_my_roles()
    Enum.each(roles, fn role -> ClusterHelper.remove_role(role) end)

    {:ok, state: :ok}
  end

  test "add role" do
    ClusterHelper.add_role(:web)
    assert ClusterHelper.get_my_roles() == [:web]
    assert ClusterHelper.get_roles(Node.self()) == [:web]
    assert ClusterHelper.get_nodes(:web) == [Node.self()]
  end

  test "add roles" do
    data = [:web, :api]
    ClusterHelper.add_roles(data)
    roles = ClusterHelper.get_my_roles()
    Enum.each(roles, fn role -> assert role in data end)
    roles = ClusterHelper.get_roles(Node.self())
    Enum.each(roles, fn role -> assert role in data end)
  end

  test "remove a role" do
    data = [:web, :api]
    ClusterHelper.add_roles(data)
    ClusterHelper.remove_role(:web)
    assert ClusterHelper.get_my_roles() == [:api]
    assert ClusterHelper.get_roles(Node.self()) == [:api]
    assert ClusterHelper.get_nodes(:web) == []
  end

  @tag timeout: 12_000
  test "don't remove role of current node" do
    ClusterHelper.add_role(:web)

    Process.sleep( 10_000 ) # need large time for pull in NodeConfig

    assert ClusterHelper.get_my_roles() == [:web]
    assert ClusterHelper.get_roles(Node.self()) == [:web]
    assert ClusterHelper.get_nodes(:web) == [Node.self()]
  end

  # TO-DO: Add remove role test
end
