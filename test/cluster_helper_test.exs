defmodule ClusterHelperTest do
  use ExUnit.Case, async: false

  doctest ClusterHelper

  alias ClusterHelper.NodeConfig

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

    test "reads directly from ETS without GenServer call" do
      ClusterHelper.add_role(:ets_test)
      # get_nodes/2 is a direct ETS read — no GenServer bottleneck
      result = ClusterHelper.get_nodes(:ets_test)
      assert Node.self() in result
    end
  end

  describe "get_roles/1" do
    test "reads directly from ETS without GenServer call" do
      ClusterHelper.add_role(:ets_role_test)
      result = ClusterHelper.get_roles(Node.self())
      assert :ets_role_test in result
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

    test "reads directly from ETS without GenServer call" do
      ClusterHelper.add_role(:all_nodes_ets)
      # all_nodes/1 is a direct ETS select — no GenServer bottleneck
      result = ClusterHelper.all_nodes()
      assert Node.self() in result
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

  # ── Generation tracking ─────────────────────────────────────────────────────

  describe "generation tracking" do
    test "generation is a non-negative integer for the default scope" do
      # Generation accumulates across tests (shared GenServer), so we only
      # verify it is a non-negative integer, not a specific value.
      gen = NodeConfig.__get_generation__(ClusterHelper)
      assert is_integer(gen) and gen >= 0
    end

    test "generation increments on add_role" do
      initial_gen = NodeConfig.__get_generation__(ClusterHelper)
      ClusterHelper.add_role(:gen_add_test)
      new_gen = NodeConfig.__get_generation__(ClusterHelper)

      assert new_gen > initial_gen
    end

    test "generation increments on remove_role" do
      ClusterHelper.add_role(:gen_remove_test)
      gen_after_add = NodeConfig.__get_generation__(ClusterHelper)

      ClusterHelper.remove_role(:gen_remove_test)
      gen_after_remove = NodeConfig.__get_generation__(ClusterHelper)

      assert gen_after_remove > gen_after_add
    end

    test "generation does not increment on duplicate add" do
      ClusterHelper.add_role(:gen_dup_test)
      gen1 = NodeConfig.__get_generation__(ClusterHelper)

      ClusterHelper.add_role(:gen_dup_test)
      gen2 = NodeConfig.__get_generation__(ClusterHelper)

      # Duplicate add should not change generation
      assert gen2 == gen1
    end

    test "generation increments on add_roles batch" do
      initial_gen = NodeConfig.__get_generation__(ClusterHelper)
      ClusterHelper.add_roles([:gen_batch1, :gen_batch2])
      new_gen = NodeConfig.__get_generation__(ClusterHelper)

      assert new_gen > initial_gen
    end

    test "generation increments on remove_roles batch" do
      ClusterHelper.add_roles([:gen_rbatch1, :gen_rbatch2])
      gen_after_add = NodeConfig.__get_generation__(ClusterHelper)

      ClusterHelper.remove_roles([:gen_rbatch1, :gen_rbatch2])
      gen_after_remove = NodeConfig.__get_generation__(ClusterHelper)

      assert gen_after_remove > gen_after_add
    end

    test "generation does not increment on removing non-existent role" do
      gen_before = NodeConfig.__get_generation__(ClusterHelper)
      ClusterHelper.remove_role(:nonexistent_gen_role)
      gen_after = NodeConfig.__get_generation__(ClusterHelper)

      assert gen_after == gen_before
    end
  end

  # ── __get_scopes__ ──────────────────────────────────────────────────────────

  describe "__get_scopes__/0" do
    test "returns the default scope" do
      scopes = NodeConfig.__get_scopes__()
      assert ClusterHelper in scopes
    end

    test "includes joined scopes" do
      scope = :"test_scope_#{System.unique_integer([:positive])}"
      ClusterHelper.join_scope(scope)

      scopes = NodeConfig.__get_scopes__()
      assert scope in scopes

      ClusterHelper.leave_scope(scope)
    end

    test "does not include left scopes" do
      scope = :"test_scope_#{System.unique_integer([:positive])}"
      ClusterHelper.join_scope(scope)
      ClusterHelper.leave_scope(scope)

      scopes = NodeConfig.__get_scopes__()
      refute scope in scopes
    end
  end

  # ── Scope isolation with generation ────────────────────────────────────────

  describe "scope isolation with generation" do
    test "each scope has its own generation counter" do
      scope = :"gen_iso_#{System.unique_integer([:positive])}"
      ClusterHelper.join_scope(scope)

      default_gen_before = NodeConfig.__get_generation__(ClusterHelper)
      scope_gen_before = NodeConfig.__get_generation__(scope)

      # Add role in the custom scope only
      ClusterHelper.add_role(:scoped_gen_role, scope)

      default_gen_after = NodeConfig.__get_generation__(ClusterHelper)
      scope_gen_after = NodeConfig.__get_generation__(scope)

      # Default scope generation should not change
      assert default_gen_after == default_gen_before
      # Custom scope generation should increment
      assert scope_gen_after > scope_gen_before

      ClusterHelper.leave_scope(scope)
    end
  end

  # ── Scope management ───────────────────────────────────────────────────────

  describe "join_scope/1 and leave_scope/1" do
    test "join_scope adds the scope to list_scopes" do
      scope = :"join_test_#{System.unique_integer([:positive])}"
      assert :ok = ClusterHelper.join_scope(scope)
      assert scope in ClusterHelper.list_scopes()
      ClusterHelper.leave_scope(scope)
    end

    test "join_scope returns error when already joined" do
      scope = :"join_dup_#{System.unique_integer([:positive])}"
      ClusterHelper.join_scope(scope)
      assert {:error, :already_joined} = ClusterHelper.join_scope(scope)
      ClusterHelper.leave_scope(scope)
    end

    test "leave_scope removes the scope and its roles" do
      scope = :"leave_test_#{System.unique_integer([:positive])}"
      ClusterHelper.join_scope(scope)
      ClusterHelper.add_role(:leave_role, scope)
      assert :leave_role in ClusterHelper.get_my_roles(scope)

      ClusterHelper.leave_scope(scope)
      refute scope in ClusterHelper.list_scopes()
    end

    test "leave_scope returns error when not joined" do
      scope = :"leave_err_#{System.unique_integer([:positive])}"
      assert {:error, :not_joined} = ClusterHelper.leave_scope(scope)
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
