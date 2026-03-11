defmodule ClusterHelper.AnyTypeRoleTest do
  @moduledoc """
  Verifies that roles can be any Elixir term, not just atoms.

  These tests use the supervised NodeConfig that the application already started.
  Each test cleans up the roles it added so subsequent tests start from a known state.
  """
  use ExUnit.Case, async: false

  # Clean up any roles added by the previous test.
  setup do
    on_exit(fn ->
      ClusterHelper.get_my_roles() |> Enum.each(&ClusterHelper.remove_role/1)
    end)
    :ok
  end

  describe "string roles" do
    test "add and retrieve a string role" do
      assert :ok = ClusterHelper.add_role("web")
      assert "web" in ClusterHelper.get_my_roles()
      assert Node.self() in ClusterHelper.get_nodes("web")
    end

    test "remove a string role" do
      ClusterHelper.add_role("api")
      ClusterHelper.remove_role("api")
      refute "api" in ClusterHelper.get_my_roles()
      assert ClusterHelper.get_nodes("api") == []
    end
  end

  describe "tuple roles" do
    test "add and retrieve a tuple role" do
      assert :ok = ClusterHelper.add_role({:shard, 1})
      assert {:shard, 1} in ClusterHelper.get_my_roles()
      assert Node.self() in ClusterHelper.get_nodes({:shard, 1})
    end

    test "remove a tuple role" do
      ClusterHelper.add_role({:shard, 2})
      ClusterHelper.remove_role({:shard, 2})
      refute {:shard, 2} in ClusterHelper.get_my_roles()
      assert ClusterHelper.get_nodes({:shard, 2}) == []
    end
  end

  describe "integer roles" do
    test "add and retrieve an integer role" do
      assert :ok = ClusterHelper.add_role(42)
      assert 42 in ClusterHelper.get_my_roles()
      assert Node.self() in ClusterHelper.get_nodes(42)
    end
  end

  describe "mixed role types in a single batch" do
    test "add_roles/1 accepts a heterogeneous list" do
      roles = [:atom_role, "string_role", {:tuple, :role}, 99]
      assert :ok = ClusterHelper.add_roles(roles)
      my_roles = ClusterHelper.get_my_roles()
      Enum.each(roles, fn r -> assert r in my_roles end)
    end

    test "remove_roles/1 accepts a heterogeneous list" do
      roles = [:atom_role, "string_role", {:tuple, :role}, 99]
      ClusterHelper.add_roles(roles)
      ClusterHelper.remove_roles(roles)
      my_roles = ClusterHelper.get_my_roles()
      Enum.each(roles, fn r -> refute r in my_roles end)
    end
  end

  describe "ETS reverse lookup" do
    test "get_roles/1 returns non-atom roles" do
      ClusterHelper.add_roles(["str", {:t, 1}])
      roles = ClusterHelper.get_roles(Node.self())
      assert "str" in roles
      assert {:t, 1} in roles
    end
  end

  describe "duplicate handling" do
    test "adding the same non-atom role twice is idempotent" do
      ClusterHelper.add_role("web")
      ClusterHelper.add_role("web")
      count = ClusterHelper.get_my_roles() |> Enum.count(&(&1 == "web"))
      assert count == 1
    end
  end
end
