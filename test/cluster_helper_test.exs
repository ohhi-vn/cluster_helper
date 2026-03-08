defmodule ClusterHelperTest do
  use ExUnit.Case, async: false

  doctest ClusterHelper

  # Reset the local node's roles before every test so tests are independent.
  setup do
    ClusterHelper.get_my_roles()
    |> Enum.each(&ClusterHelper.remove_role/1)

    :ok
  end

  describe "add_role/1" do
    test "records the role in all three query surfaces" do
      ClusterHelper.add_role(:web)

      assert ClusterHelper.get_my_roles() == [:web]
      assert ClusterHelper.get_roles(Node.self()) == [:web]
      assert ClusterHelper.get_nodes(:web) == [Node.self()]
    end

    test "duplicate role is ignored" do
      ClusterHelper.add_role(:web)
      ClusterHelper.add_role(:web)

      assert ClusterHelper.get_my_roles() == [:web]
    end
  end

  describe "add_roles/1" do
    test "all supplied roles are added" do
      roles = [:web, :api]
      ClusterHelper.add_roles(roles)

      assert Enum.sort(ClusterHelper.get_my_roles()) == Enum.sort(roles)
      assert Enum.sort(ClusterHelper.get_roles(Node.self())) == Enum.sort(roles)
    end

    test "duplicate roles in the list are deduplicated" do
      ClusterHelper.add_roles([:web, :web, :api])

      assert length(ClusterHelper.get_my_roles()) == 2
    end
  end

  describe "remove_role/1" do
    test "removes the role from all query surfaces" do
      ClusterHelper.add_roles([:web, :api])
      ClusterHelper.remove_role(:web)

      assert ClusterHelper.get_my_roles() == [:api]
      assert ClusterHelper.get_roles(Node.self()) == [:api]
      assert ClusterHelper.get_nodes(:web) == []
    end

    test "removing a non-existent role is a no-op" do
      ClusterHelper.add_role(:web)
      ClusterHelper.remove_role(:nonexistent)

      assert ClusterHelper.get_my_roles() == [:web]
    end
  end

  describe "remove_roles/1" do
    test "removes all supplied roles at once" do
      ClusterHelper.add_roles([:web, :api, :cache])
      ClusterHelper.remove_roles([:web, :api])

      assert ClusterHelper.get_my_roles() == [:cache]
      assert ClusterHelper.get_nodes(:web) == []
      assert ClusterHelper.get_nodes(:api) == []
      assert ClusterHelper.get_nodes(:cache) == [Node.self()]
    end
  end

  describe "get_nodes/1" do
    test "returns empty list for unknown role" do
      assert ClusterHelper.get_nodes(:unknown_role) == []
    end
  end

  describe "local_node?/1" do
    test "returns true for the local node" do
      assert ClusterHelper.local_node?(Node.self()) == true
    end

    test "returns false for a non-local atom" do
      assert ClusterHelper.local_node?(:"other@127.0.0.1") == false
    end
  end

  describe "all_nodes/0" do
    test "includes the current node once a role is set" do
      ClusterHelper.add_role(:web)

      assert Node.self() in ClusterHelper.all_nodes()
    end

    test "contains no duplicates" do
      ClusterHelper.add_roles([:web, :api])
      nodes = ClusterHelper.all_nodes()

      assert nodes == Enum.uniq(nodes)
    end
  end

  # This test verifies that the periodic pull (every 7 s by default) does NOT
  # evict the local node's own roles. Tag it with a generous timeout.
  @tag timeout: 12_000
  test "periodic pull does not remove local node roles" do
    ClusterHelper.add_role(:web)
    # Wait long enough for at least one full pull cycle (default 7 s).
    Process.sleep(10_000)

    assert ClusterHelper.get_my_roles() == [:web]
    assert ClusterHelper.get_roles(Node.self()) == [:web]
    assert ClusterHelper.get_nodes(:web) == [Node.self()]
  end
end
