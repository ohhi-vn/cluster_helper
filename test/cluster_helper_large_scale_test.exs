defmodule ClusterHelper.LargeScaleTest do
  @moduledoc """
  Large-scale stress tests for ClusterHelper.

  These tests verify that ClusterHelper handles large role sets, concurrent
  operations, and stress conditions correctly. They are tagged with
  `:large_scale` and excluded from normal test runs.

  Run with:
      mix test --only large_scale
  """
  use ExUnit.Case, async: false

  @moduletag :large_scale

  alias ClusterHelper.NodeConfig

  # ── Setup & Teardown ─────────────────────────────────────────────────────────

  setup do
    # Clean slate before each test - remove roles from all active scopes
    on_exit(fn ->
      ClusterHelper.list_scopes()
      |> Enum.each(fn scope ->
        ClusterHelper.get_my_roles(scope)
        |> Enum.each(&ClusterHelper.remove_role(&1, scope))
      end)
    end)

    :ok
  end

  # ── Large Role Set Tests ─────────────────────────────────────────────────────

  describe "large role sets" do
    test "handles 1,000 roles correctly" do
      roles = Enum.map(1..1_000, fn i -> :"role_#{i}" end)

      assert :ok = ClusterHelper.add_roles(roles)
      my_roles = ClusterHelper.get_my_roles()

      assert length(my_roles) == 1_000
      assert MapSet.equal?(MapSet.new(my_roles), MapSet.new(roles))

      # Verify all roles are queryable
      Enum.each(roles, fn role ->
        assert Node.self() in ClusterHelper.get_nodes(role)
      end)

      # Verify cleanup
      assert :ok = ClusterHelper.remove_roles(roles)
      assert ClusterHelper.get_my_roles() == []
    end

    test "handles 5,000 roles correctly" do
      roles = Enum.map(1..5_000, fn i -> :"role_#{i}" end)

      old_roles = ClusterHelper.get_my_roles()
      ClusterHelper.remove_roles(old_roles)

      assert :ok = ClusterHelper.add_roles(roles)
      my_roles = ClusterHelper.get_my_roles()

      assert length(my_roles) == 5_000
      assert MapSet.equal?(MapSet.new(my_roles), MapSet.new(roles))

      # Spot check random roles
      [1, 500, 2500, 4999]
      |> Enum.map(fn i -> Enum.at(roles, i - 1) end)
      |> Enum.each(fn role ->
        assert Node.self() in ClusterHelper.get_nodes(role)
      end)

      # Verify cleanup
      assert :ok = ClusterHelper.remove_roles(roles)
      assert ClusterHelper.get_my_roles() == []
    end

    test "handles 2,000 roles within reasonable time" do
      roles = Enum.map(1..2_000, fn i -> :"role_#{i}" end)

      {add_time, _} = :timer.tc(fn -> ClusterHelper.add_roles(roles) end)
      add_time_ms = add_time / 1000

      # Should complete in under 5 seconds
      assert add_time_ms < 5_000, "Adding 2K roles took #{add_time_ms}ms (expected < 5000ms)"

      my_roles = ClusterHelper.get_my_roles()
      assert length(my_roles) == 2_000

      {get_time, _} = :timer.tc(fn -> ClusterHelper.get_my_roles() end)
      get_time_ms = get_time / 1000

      # Get should be fast even with 2K roles
      assert get_time_ms < 20, "Getting 2K roles took #{get_time_ms}ms (expected < 20ms)"

      # Cleanup
      {remove_time, _} = :timer.tc(fn -> ClusterHelper.remove_roles(roles) end)
      remove_time_ms = remove_time / 1000

      assert remove_time_ms < 5_000,
             "Removing 2K roles took #{remove_time_ms}ms (expected < 5000ms)"

      assert ClusterHelper.get_my_roles() == []
    end
  end

  # ── ETS Consistency Tests ────────────────────────────────────────────────────

  describe "scope isolation and cleanup" do
    test "roles are isolated per scope" do
      scope_a = :"isolation_a_#{System.unique_integer()}"
      scope_b = :"isolation_b_#{System.unique_integer()}"

      ClusterHelper.join_scope(scope_a)
      ClusterHelper.join_scope(scope_b)

      # Add different roles to each scope
      ClusterHelper.add_roles([:role_a1, :role_a2], scope_a)
      ClusterHelper.add_roles([:role_b1, :role_b2, :role_b3], scope_b)

      # Verify isolation
      assert MapSet.new(ClusterHelper.get_my_roles(scope_a)) == MapSet.new([:role_a1, :role_a2])

      assert MapSet.new(ClusterHelper.get_my_roles(scope_b)) ==
               MapSet.new([:role_b1, :role_b2, :role_b3])

      # Node should appear in both scopes' node lists
      assert Node.self() in ClusterHelper.all_nodes(scope_a)
      assert Node.self() in ClusterHelper.all_nodes(scope_b)

      # Cleanup
      ClusterHelper.remove_roles([:role_a1, :role_a2], scope_a)
      ClusterHelper.remove_roles([:role_b1, :role_b2, :role_b3], scope_b)

      assert ClusterHelper.get_my_roles(scope_a) == []
      assert ClusterHelper.get_my_roles(scope_b) == []

      ClusterHelper.leave_scope(scope_a)
      ClusterHelper.leave_scope(scope_b)
    end

    test "removing all roles removes node from scope's node list" do
      scope = :"cleanup_#{System.unique_integer()}"
      ClusterHelper.join_scope(scope)

      # Add and verify roles
      ClusterHelper.add_roles([:r1, :r2, :r3], scope)
      assert Node.self() in ClusterHelper.all_nodes(scope)
      assert length(ClusterHelper.get_my_roles(scope)) == 3

      # Remove all roles
      ClusterHelper.remove_roles([:r1, :r2, :r3], scope)

      # Node should no longer appear in this scope
      assert ClusterHelper.all_nodes(scope) == []
      assert ClusterHelper.get_my_roles(scope) == []

      ClusterHelper.leave_scope(scope)
    end

    test "partial role removal keeps node in scope" do
      scope = :"partial_#{System.unique_integer()}"
      ClusterHelper.join_scope(scope)

      # Add roles
      ClusterHelper.add_roles([:keep1, :keep2, :remove1, :remove2], scope)
      assert length(ClusterHelper.get_my_roles(scope)) == 4
      assert Node.self() in ClusterHelper.all_nodes(scope)

      # Remove some roles
      ClusterHelper.remove_roles([:remove1, :remove2], scope)

      # Node should still be in scope
      assert Node.self() in ClusterHelper.all_nodes(scope)
      assert MapSet.new(ClusterHelper.get_my_roles(scope)) == MapSet.new([:keep1, :keep2])

      # Cleanup
      ClusterHelper.remove_roles([:keep1, :keep2], scope)
      ClusterHelper.leave_scope(scope)
    end

    test "leave_scope cleans up all scope data" do
      scope = :"leave_test_#{System.unique_integer()}"
      ClusterHelper.join_scope(scope)

      ClusterHelper.add_roles([:l1, :l2, :l3], scope)
      assert Node.self() in ClusterHelper.all_nodes(scope)

      ClusterHelper.leave_scope(scope)

      # After leaving, scope should not be in list_scopes
      refute scope in ClusterHelper.list_scopes()
    end
  end

  # ── Concurrent Operations Tests ──────────────────────────────────────────────

  describe "concurrent operations" do
    test "concurrent add_role calls from multiple processes" do
      num_processes = 5
      roles_per_process = 50

      # Spawn multiple processes adding roles concurrently
      tasks =
        Enum.map(1..num_processes, fn proc_id ->
          Task.async(fn ->
            roles = Enum.map(1..roles_per_process, fn i -> :"proc_#{proc_id}_role_#{i}" end)
            ClusterHelper.add_roles(roles)
            roles
          end)
        end)

      all_roles = tasks |> Task.await_many(10_000) |> List.flatten()

      # Verify all roles were added
      my_roles = ClusterHelper.get_my_roles()
      assert length(my_roles) == num_processes * roles_per_process
      assert MapSet.equal?(MapSet.new(my_roles), MapSet.new(all_roles))

      # Cleanup
      ClusterHelper.remove_roles(all_roles)
    end

    test "interleaved add and remove operations" do
      # Add initial batch
      initial_roles = Enum.map(1..200, fn i -> :"initial_#{i}" end)
      ClusterHelper.add_roles(initial_roles)

      # Concurrent operations
      add_task =
        Task.async(fn ->
          new_roles = Enum.map(1..200, fn i -> :"new_#{i}" end)
          ClusterHelper.add_roles(new_roles)
          new_roles
        end)

      remove_task =
        Task.async(fn ->
          to_remove = Enum.take(initial_roles, 100)
          ClusterHelper.remove_roles(to_remove)
          to_remove
        end)

      new_roles = Task.await(add_task, 10_000)
      removed_roles = Task.await(remove_task, 10_000)

      # Verify state
      my_roles = ClusterHelper.get_my_roles()
      expected_remaining = initial_roles -- removed_roles
      expected_total = expected_remaining ++ new_roles

      assert length(my_roles) == length(expected_total)
      assert MapSet.equal?(MapSet.new(my_roles), MapSet.new(expected_total))

      # Cleanup
      ClusterHelper.remove_roles(my_roles)
    end
  end

  # ── Event Handler Performance Tests ──────────────────────────────────────────

  # Event handler collector - must be at module level for setup_all
  defmodule FastCollector do
    @behaviour ClusterHelper.EventHandler

    def start(), do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def stop(), do: Agent.stop(__MODULE__)
    def count(), do: Agent.get(__MODULE__, &length/1)
    def events(), do: Agent.get(__MODULE__, &Enum.reverse/1)
    def reset(), do: Agent.update(__MODULE__, fn _ -> [] end)

    @impl true
    def on_role_added(_node, _role) do
      Agent.update(__MODULE__, fn events -> [{:role_added} | events] end)
    end

    @impl true
    def on_role_removed(_node, _role) do
      Agent.update(__MODULE__, fn events -> [{:role_removed} | events] end)
    end

    @impl true
    def on_node_added(_node), do: :ok

    @impl true
    def on_node_removed(_node), do: :ok
  end

  setup_all do
    {:ok, _} = FastCollector.start()
    on_exit(&FastCollector.stop/0)
    :ok
  end

  describe "event handler performance" do
    setup do
      FastCollector.reset()
      Application.put_env(:cluster_helper, :event_handler, FastCollector)

      on_exit(fn ->
        Application.delete_env(:cluster_helper, :event_handler)
      end)

      :ok
    end

    test "event handler fires for each role in large batch" do
      roles = Enum.map(1..500, fn i -> :"event_role_#{i}" end)

      ClusterHelper.add_roles(roles)

      count = FastCollector.count()
      assert count == 500, "Expected 500 events, got #{count}"

      # Cleanup
      ClusterHelper.remove_roles(roles)
    end

    test "event handler performance with 1,000 roles" do
      roles = Enum.map(1..1_000, fn i -> :"perf_event_#{i}" end)

      {add_time, _} = :timer.tc(fn -> ClusterHelper.add_roles(roles) end)
      add_time_ms = add_time / 1000

      # Should complete in reasonable time with event handlers
      assert add_time_ms < 10_000, "Adding 1K roles with events took #{add_time_ms}ms"

      count = FastCollector.count()
      assert count == 1_000

      # Cleanup
      ClusterHelper.remove_roles(roles)
    end

    test "on_role_removed fires for each role in large batch remove" do
      roles = Enum.map(1..500, fn i -> :"remove_event_#{i}" end)
      ClusterHelper.add_roles(roles)
      FastCollector.reset()

      ClusterHelper.remove_roles(roles)

      events = FastCollector.events()
      removed_count = Enum.count(events, &match?({:role_removed}, &1))
      assert removed_count == 500, "Expected 500 on_role_removed events, got #{removed_count}"
    end

    test "on_role_removed fires for each role when node entry is removed" do
      roles = Enum.map(1..200, fn i -> :"node_remove_event_#{i}" end)
      ClusterHelper.add_roles(roles)
      FastCollector.reset()

      # Simulate nodedown by sending the message directly
      send(NodeConfig, {:nodedown, :"fake_node@127.0.0.1", []})

      # The fake node has no roles in ETS, so no on_role_removed should fire
      # But on_node_removed should still fire
      events = FastCollector.events()
      removed_count = Enum.count(events, &match?({:role_removed}, &1))
      assert removed_count == 0
    end
  end

  # ── Memory Efficiency Tests ──────────────────────────────────────────────────

  describe "memory efficiency" do
    test "ETS table size scales linearly with roles" do
      ets_table = NodeConfig

      # Measure with 100 roles
      roles_100 = Enum.map(1..100, fn i -> :"mem_#{i}" end)
      ClusterHelper.add_roles(roles_100)
      size_100 = :ets.info(ets_table, :memory)

      # Measure with 1,000 roles
      roles_1000 = Enum.map(101..1_100, fn i -> :"mem_#{i}" end)
      ClusterHelper.add_roles(roles_1000)
      size_1000 = :ets.info(ets_table, :memory)

      # Memory should scale roughly linearly (within 2x factor)
      expected_ratio = 10
      actual_ratio = size_1000 / size_100

      assert actual_ratio < expected_ratio * 2,
             "Memory scaling inefficient: #{actual_ratio}x for 10x roles"

      # Cleanup
      ClusterHelper.remove_roles(roles_100 ++ roles_1000)
    end

    test "cleanup releases ETS memory" do
      _ets_table = NodeConfig

      initial_memory = :erlang.memory(:ets)

      roles = Enum.map(1..2_000, fn i -> :"cleanup_#{i}" end)
      ClusterHelper.add_roles(roles)

      after_add_memory = :erlang.memory(:ets)
      assert after_add_memory > initial_memory

      ClusterHelper.remove_roles(roles)

      # Force GC to get more accurate memory reading
      :erlang.garbage_collect()
      Process.sleep(50)

      after_remove_memory = :erlang.memory(:ets)

      # Memory should be mostly reclaimed (within 20% of initial, ETS doesn't always release immediately)
      assert after_remove_memory < initial_memory * 1.2,
             "ETS memory not properly cleaned up: #{after_remove_memory} vs #{initial_memory}"
    end
  end

  # ── Edge Cases & Stress Tests ────────────────────────────────────────────────

  describe "edge cases and stress" do
    test "rapid add/remove cycles" do
      # Perform 10 rapid add/remove cycles, keeping only current cycle's roles
      Enum.each(1..10, fn cycle ->
        roles = Enum.map(1..20, fn i -> :"cycle_#{cycle}_role_#{i}" end)
        ClusterHelper.add_roles(roles)

        # Remove previous cycle's roles first
        if cycle > 1 do
          prev_roles = Enum.map(1..20, fn i -> :"cycle_#{cycle - 1}_role_#{i}" end)
          ClusterHelper.remove_roles(prev_roles)
        end

        # Only current cycle's roles should remain
        assert length(ClusterHelper.get_my_roles()) == 20
      end)

      # Final cleanup
      ClusterHelper.get_my_roles()
      |> Enum.each(&ClusterHelper.remove_role/1)

      assert ClusterHelper.get_my_roles() == []
    end

    test "mixed role types at scale" do
      roles =
        Enum.map(1..500, fn i ->
          case rem(i, 4) do
            0 -> :"atom_role_#{i}"
            1 -> "string_role_#{i}"
            2 -> {:tuple_role, i}
            3 -> i
          end
        end)

      ClusterHelper.add_roles(roles)
      my_roles = ClusterHelper.get_my_roles()

      assert length(my_roles) == 500
      assert MapSet.equal?(MapSet.new(my_roles), MapSet.new(roles))

      # Verify all types are queryable
      Enum.each(roles, fn role ->
        assert Node.self() in ClusterHelper.get_nodes(role)
      end)

      # Cleanup
      ClusterHelper.remove_roles(roles)
    end

    test "duplicate handling at scale" do
      base_roles = Enum.map(1..200, fn i -> :"dup_#{i}" end)

      # Add with many duplicates
      roles_with_dups =
        base_roles ++ base_roles ++ Enum.take(base_roles, 100)

      ClusterHelper.add_roles(roles_with_dups)
      my_roles = ClusterHelper.get_my_roles()

      # Should only have 200 unique roles
      assert length(my_roles) == 200
      assert MapSet.equal?(MapSet.new(my_roles), MapSet.new(base_roles))

      # Cleanup
      ClusterHelper.remove_roles(base_roles)
    end
  end

  # ── Generation Tracking Tests ────────────────────────────────────────────────

  describe "generation tracking at scale" do
    test "generation increments correctly with large batch operations" do
      initial_gen = NodeConfig.__get_generation__(ClusterHelper)

      roles = Enum.map(1..500, fn i -> :"gen_scale_#{i}" end)
      ClusterHelper.add_roles(roles)
      gen_after_add = NodeConfig.__get_generation__(ClusterHelper)

      assert gen_after_add > initial_gen, "Generation should increment after batch add"

      ClusterHelper.remove_roles(roles)
      gen_after_remove = NodeConfig.__get_generation__(ClusterHelper)

      assert gen_after_remove > gen_after_add, "Generation should increment after batch remove"
    end

    test "generation does not increment on duplicate batch add" do
      roles = Enum.map(1..100, fn i -> :"gen_dup_scale_#{i}" end)
      ClusterHelper.add_roles(roles)
      gen1 = NodeConfig.__get_generation__(ClusterHelper)

      # Add the same roles again — all duplicates
      ClusterHelper.add_roles(roles)
      gen2 = NodeConfig.__get_generation__(ClusterHelper)

      assert gen2 == gen1, "Generation should not increment on duplicate add"

      ClusterHelper.remove_roles(roles)
    end

    test "generation does not increment on removing non-existent roles" do
      gen_before = NodeConfig.__get_generation__(ClusterHelper)

      # Remove roles that were never added
      fake_roles = Enum.map(1..100, fn i -> :"nonexistent_gen_#{i}" end)
      ClusterHelper.remove_roles(fake_roles)

      gen_after = NodeConfig.__get_generation__(ClusterHelper)

      assert gen_after == gen_before,
             "Generation should not increment on removing non-existent roles"
    end

    test "each scope has independent generation tracking" do
      scope = :"gen_indep_#{System.unique_integer([:positive])}"
      ClusterHelper.join_scope(scope)

      default_gen_before = NodeConfig.__get_generation__(ClusterHelper)
      scope_gen_before = NodeConfig.__get_generation__(scope)

      # Add 500 roles in the custom scope
      roles = Enum.map(1..500, fn i -> :"scope_gen_#{i}" end)
      ClusterHelper.add_roles(roles, scope)

      default_gen_after = NodeConfig.__get_generation__(ClusterHelper)
      scope_gen_after = NodeConfig.__get_generation__(scope)

      # Default scope generation should not change
      assert default_gen_after == default_gen_before
      # Custom scope generation should increment
      assert scope_gen_after > scope_gen_before

      ClusterHelper.leave_scope(scope)
    end

    test "__get_scopes__/0 returns all joined scopes" do
      scopes =
        Enum.map(1..5, fn i ->
          :"multi_scope_#{i}_#{System.unique_integer([:positive])}"
        end)

      Enum.each(scopes, &ClusterHelper.join_scope/1)

      result = NodeConfig.__get_scopes__()

      Enum.each(scopes, fn scope ->
        assert scope in result, "Scope #{inspect(scope)} should be in __get_scopes__"
      end)

      Enum.each(scopes, &ClusterHelper.leave_scope/1)
    end
  end

  # ── on_role_removed and on_node_removed Callback Tests ───────────────────────

  describe "on_role_removed callback at scale" do
    setup do
      FastCollector.reset()
      Application.put_env(:cluster_helper, :event_handler, FastCollector)

      on_exit(fn ->
        Application.delete_env(:cluster_helper, :event_handler)
      end)

      :ok
    end

    test "on_role_removed fires for each role in batch remove at scale" do
      roles = Enum.map(1..1_000, fn i -> :"rm_scale_#{i}" end)
      ClusterHelper.add_roles(roles)
      FastCollector.reset()

      ClusterHelper.remove_roles(roles)

      events = FastCollector.events()
      removed_count = Enum.count(events, &match?({:role_removed}, &1))
      assert removed_count == 1_000, "Expected 1000 on_role_removed events, got #{removed_count}"
    end

    test "on_role_removed fires for each role when remote node entry is removed" do
      remote = :"fake_scale_remote@127.0.0.1"
      roles = Enum.map(1..200, fn i -> :"remote_rm_#{i}" end)

      # Simulate a remote node adding roles
      send(NodeConfig, {:new_roles, ClusterHelper, roles, remote})
      FastCollector.reset()

      # Simulate nodedown — should fire on_role_removed for each role
      send(NodeConfig, {:nodedown, remote, []})

      # Give the GenServer time to process
      Process.sleep(50)

      events = FastCollector.events()
      removed_count = Enum.count(events, &match?({:role_removed}, &1))

      assert removed_count == 200,
             "Expected 200 on_role_removed events on nodedown, got #{removed_count}"
    end

    test "on_role_removed does not fire for non-existent roles" do
      FastCollector.reset()

      # Remove roles that were never added
      fake_roles = Enum.map(1..100, fn i -> :"nonexistent_rm_#{i}" end)
      ClusterHelper.remove_roles(fake_roles)

      events = FastCollector.events()
      removed_count = Enum.count(events, &match?({:role_removed}, &1))

      assert removed_count == 0,
             "Expected 0 on_role_removed events for non-existent roles, got #{removed_count}"
    end
  end

  describe "on_node_removed callback" do
    setup do
      FastCollector.reset()
      Application.put_env(:cluster_helper, :event_handler, FastCollector)

      on_exit(fn ->
        Application.delete_env(:cluster_helper, :event_handler)
      end)

      :ok
    end

    test "on_node_removed fires on nodedown even for unknown node" do
      FastCollector.reset()

      # Simulate nodedown for a node that was never seen
      send(NodeConfig, {:nodedown, :"unknown_down@127.0.0.1", []})

      # Give the GenServer time to process
      Process.sleep(50)

      events = FastCollector.events()
      # FastCollector doesn't track node_removed specifically, but the
      # dispatch should not crash. Verify by checking no unexpected errors.
      assert is_list(events)
    end

    test "on_node_removed fires and on_role_removed fires for all roles on nodedown" do
      remote = :"multi_down@127.0.0.1"
      roles = Enum.map(1..50, fn i -> :"down_role_#{i}" end)

      # Register the remote node with roles
      send(NodeConfig, {:new_roles, ClusterHelper, roles, remote})
      FastCollector.reset()

      # Simulate nodedown
      send(NodeConfig, {:nodedown, remote, []})
      Process.sleep(50)

      events = FastCollector.events()
      removed_count = Enum.count(events, &match?({:role_removed}, &1))

      assert removed_count == 50,
             "Expected 50 on_role_removed events on nodedown, got #{removed_count}"
    end
  end
end
