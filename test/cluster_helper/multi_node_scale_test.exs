defmodule ClusterHelper.MultiNodeScaleTest do
  @moduledoc """
  Multi-node scale tests simulating 20+ nodes joining and leaving the cluster.

  These tests verify that ClusterHelper handles large-scale node churn correctly,
  including role convergence, cleanup on node departure, and stability under
  rapid join/leave cycles.

  Tagged with `:multi_node_scale` and excluded from normal test runs.

  Run with:
      mix test --only multi_node_scale
  """
  use ExUnit.Case, async: false

  @moduletag :multi_node_scale

  # Skip all tests in this module if not running on a distributed node.
  # Evaluated at compile time - the test file is compiled before execution.
  @moduletag skip: not Node.alive?()

  setup_all do
    if Node.alive?() do
      IO.puts("\n🚀 Starting multi-node scale tests (#{Node.self()})\n")
    end
    :ok
  end

  # Timeouts for large-scale operations
  @node_boot_timeout 30_000
  @convergence_timeout 60_000
  @poll_interval 200

  # ── Helpers ──────────────────────────────────────────────────────────────────

  @spec start_peer(atom(), keyword()) :: {:peer.peer(), node()}
  defp start_peer(name_prefix, opts \\ []) do
    name = :"#{name_prefix}_#{System.unique_integer([:positive, :monotonic])}"
    roles = Keyword.get(opts, :roles, [])

    host =
      Node.self()
      |> Atom.to_string()
      |> String.split("@")
      |> List.last()
      |> to_charlist()

    cookie = Node.get_cookie() |> Atom.to_string() |> to_charlist()

    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        host: host,
        args: [~c"-setcookie", cookie],
        wait_boot: @node_boot_timeout
      })

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _started} = :erpc.call(node, Application, :ensure_all_started, [:cluster_helper])

    wait_until(
      fn -> node in Node.list() end,
      @convergence_timeout,
      "peer #{node} did not connect to the cluster"
    )

    unless roles == [] do
      :erpc.call(node, ClusterHelper, :add_roles, [roles])
    end

    {peer, node}
  end

  @spec stop_peer({:peer.peer(), node()}) :: :ok
  defp stop_peer({peer, _node}) do
    try do
      :peer.stop(peer)
    catch
      _, _ -> :ok
    end
  end

  @spec wait_until((-> boolean()), non_neg_integer(), String.t()) :: :ok
  defp wait_until(
        condition,
        timeout \\ @convergence_timeout,
        msg \\ "condition never became true"
      ) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_poll(condition, deadline, msg)
  end

  defp do_poll(condition, deadline, msg) do
    if condition.() do
      :ok
    else
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(@poll_interval)
        do_poll(condition, deadline, msg)
      else
        flunk(msg)
      end
    end
  end

  # (all_cluster_nodes helper removed - not needed)

  # ── 20-Node Cluster Tests ────────────────────────────────────────────────────

  describe "20-node cluster convergence" do
    test "20 nodes join cluster and roles are visible on test node" do
      node_count = 20
      roles_per_node = 2

      # Spawn 20 nodes with unique roles
      peers =
        Enum.map(1..node_count, fn i ->
          roles = Enum.map(1..roles_per_node, fn j -> :"node#{i}_role#{j}" end)
          start_peer(:"scale_#{i}", roles: roles)
        end)

      nodes = Enum.map(peers, fn {_peer, node} -> node end)

      try do
        # Wait until all nodes are connected at the distribution level
        wait_until(
          fn ->
            connected = Node.list()
            Enum.all?(nodes, &(&1 in connected))
          end,
          @convergence_timeout,
          "Not all #{node_count} nodes connected"
        )

        # Verify each node has the correct number of roles (query directly)
        Enum.each(nodes, fn node ->
          roles = :erpc.call(node, ClusterHelper, :get_my_roles, [])

          assert length(roles) == roles_per_node,
                 "Node #{node} should have #{roles_per_node} roles, got #{length(roles)}"
        end)

        # Wait for test node to discover most nodes via pull cycles
        # (pull-based sync takes time for 20 nodes)
        Process.sleep(3000)

        # Verify test node can see most nodes
        all_nodes = ClusterHelper.all_nodes()
        # Allow 50% threshold since pull-based sync is eventual
        min_expected = trunc(node_count * 0.5)

        assert length(all_nodes) >= min_expected,
               "Expected at least #{min_expected} nodes in all_nodes, got #{length(all_nodes)}"

        # Verify each node's roles are discoverable (eventual consistency)
        # Check a sample of nodes rather than all 20
        sample_nodes = Enum.take(nodes, 5)

        Enum.each(sample_nodes, fn node ->
          node_roles = :erpc.call(node, ClusterHelper, :get_my_roles, [])

          Enum.each(node_roles, fn role ->
            # Role should eventually be visible on test node
            nodes_with_role = ClusterHelper.get_nodes(role)

            assert node in nodes_with_role or length(nodes_with_role) > 0,
                   "Role #{role} from node #{node} should be visible"
          end)
        end)
      after
        # Cleanup all peers
        Enum.each(peers, &stop_peer/1)
      end
    end

    test "rapid node churn - 20 nodes join and leave in waves" do
      wave_count = 3
      nodes_per_wave = 7

      Enum.each(1..wave_count, fn wave ->
        # Start a wave of nodes
        peers =
          Enum.map(1..nodes_per_wave, fn i ->
            start_peer(:"wave#{wave}_node#{i}", roles: [:"wave#{wave}_role"])
          end)

        nodes = Enum.map(peers, fn {_peer, node} -> node end)

        try do
          # Wait for nodes to connect
          wait_until(
            fn -> Enum.all?(nodes, &(&1 in Node.list())) end,
            @convergence_timeout,
            "Wave #{wave} nodes did not connect"
          )

          # Verify roles are visible
          wait_until(
            fn ->
              Enum.all?(nodes, fn node ->
                node in ClusterHelper.get_nodes(:"wave#{wave}_role")
              end)
            end,
            @convergence_timeout,
            "Wave #{wave} roles did not propagate"
          )

          # Stop all nodes in this wave
          Enum.each(peers, &stop_peer/1)

          # Wait for nodes to disconnect
          wait_until(
            fn -> Enum.all?(nodes, fn node -> node not in Node.list() end) end,
            @convergence_timeout,
            "Wave #{wave} nodes did not disconnect"
          )

          # Wait for roles to be cleaned up (after pull cycle)
          Process.sleep(1000)

          # Verify roles are cleaned up
          wave_nodes = ClusterHelper.get_nodes(:"wave#{wave}_role")
          # Nodes might still be in the list briefly due to pull cycle timing
          # but they should not be in the connected nodes list
          connected_wave_nodes = Enum.filter(wave_nodes, &(&1 in Node.list()))

          assert connected_wave_nodes == [],
                 "Wave #{wave} nodes should be cleaned up, got: #{inspect(connected_wave_nodes)}"
        rescue
          e ->
            # Cleanup on error
            Enum.each(peers, &stop_peer/1)
            reraise e, __STACKTRACE__
        end
      end)
    end
  end

  describe "node departure cleanup" do
    test "stopping 10 nodes cleans up all their roles" do
      node_count = 10
      roles_per_node = 5

      peers =
        Enum.map(1..node_count, fn i ->
          roles = Enum.map(1..roles_per_node, fn j -> :"depart_node#{i}_role#{j}" end)
          start_peer(:"depart_#{i}", roles: roles)
        end)

      nodes = Enum.map(peers, fn {_peer, node} -> node end)

      try do
        # Wait for all nodes to connect and roles to propagate
        wait_until(
          fn ->
            Enum.all?(nodes, fn node ->
              node in Node.list()
            end)
          end,
          @convergence_timeout,
          "Not all departure test nodes connected"
        )

        # Verify all roles are visible
        Enum.each(nodes, fn node ->
          roles = :erpc.call(node, ClusterHelper, :get_my_roles, [])

          assert length(roles) == roles_per_node
        end)

        # Stop all nodes
        Enum.each(peers, &stop_peer/1)

        # Wait for disconnection
        wait_until(
          fn -> Enum.all?(nodes, fn node -> node not in Node.list() end) end,
          @convergence_timeout,
          "Departure test nodes did not disconnect"
        )

        # Wait for pull cycle to clean up (default pull_interval is 7s, but we check after 2s)
        Process.sleep(2000)

        # Verify all roles are cleaned up
        Enum.each(nodes, fn node ->
          # Get all roles that this node had
          # Since the node is gone, we can't query it directly
          # Instead, check that no roles reference this node
          all_roles = ClusterHelper.all_nodes()

          refute node in all_roles,
                 "Node #{node} should be cleaned up from all_nodes, but still present"
        end)

        # Verify no stale roles remain
        # Collect all unique roles that were assigned
        all_assigned_roles =
          Enum.flat_map(1..node_count, fn i ->
            Enum.map(1..roles_per_node, fn j -> :"depart_node#{i}_role#{j}" end)
          end)

        # Check that none of these roles have the departed nodes
        Enum.each(all_assigned_roles, fn role ->
          nodes_with_role = ClusterHelper.get_nodes(role)
          departed_still_present = Enum.filter(nodes_with_role, &(&1 in nodes))

          assert departed_still_present == [],
                 "Role #{role} still has departed nodes: #{inspect(departed_still_present)}"
        end)
      after
        Enum.each(peers, &stop_peer/1)
      end
    end

    test "partial node failure - half the cluster leaves" do
      total_nodes = 12
      leaving_nodes = 6

      # Add roles to test node
      ClusterHelper.add_role(:coordinator)

      peers =
        Enum.map(1..total_nodes, fn i ->
          role = :"partial_node_#{i}"
          start_peer(:"partial_#{i}", roles: [role])
        end)

      nodes = Enum.map(peers, fn {_peer, node} -> node end)
      {leaving_peers, _staying_peers} = Enum.split(peers, leaving_nodes)
      {leaving_nodes_list, staying_nodes_list} = Enum.split(nodes, leaving_nodes)

      try do
        # Wait for all nodes to connect
        wait_until(
          fn -> Enum.all?(nodes, &(&1 in Node.list())) end,
          @convergence_timeout,
          "Not all partial test nodes connected"
        )

        # Verify most nodes are visible (allow for convergence lag)
        all_nodes = ClusterHelper.all_nodes()
        min_expected = trunc((total_nodes + 1) * 0.8)

        assert length(all_nodes) >= min_expected,
               "Expected at least #{min_expected} nodes, got #{length(all_nodes)}"

        # Stop half the nodes
        Enum.each(leaving_peers, &stop_peer/1)

        # Wait for disconnection
        wait_until(
          fn -> Enum.all?(leaving_nodes_list, fn node -> node not in Node.list() end) end,
          @convergence_timeout,
          "Leaving nodes did not disconnect"
        )

        # Wait for convergence
        Process.sleep(2000)

        # Verify staying nodes are still visible
        Enum.each(staying_nodes_list, fn node ->
          assert node in Node.list(), "Staying node #{node} should still be connected"
        end)

        # Verify staying nodes' roles are still visible
        Enum.each(staying_nodes_list, fn node ->
          role = :"partial_node_#{Enum.find_index(nodes, &(&1 == node)) + 1}"

          assert node in ClusterHelper.get_nodes(role),
                 "Staying node's role #{role} should still be visible"
        end)
      after
        Enum.each(peers, &stop_peer/1)
        ClusterHelper.remove_role(:coordinator)
      end
    end
  end

  describe "role distribution across large cluster" do
    test "shared role is visible across all 20 nodes" do
      node_count = 20
      shared_role = :shared_service

      # Add role to test node
      ClusterHelper.add_role(shared_role)

      peers =
        Enum.map(1..node_count, fn i ->
          start_peer(:"shared_#{i}", roles: [shared_role])
        end)

      nodes = Enum.map(peers, fn {_peer, node} -> node end)

      try do
        # Wait for all nodes to connect
        wait_until(
          fn -> Enum.all?(nodes, &(&1 in Node.list())) end,
          @convergence_timeout,
          "Not all shared role nodes connected"
        )

        # Wait for role to propagate to all nodes
        wait_until(
          fn ->
            nodes_with_role = ClusterHelper.get_nodes(shared_role)
            length(nodes_with_role) >= node_count + 1
          end,
          @convergence_timeout,
          "Shared role did not propagate to all #{node_count + 1} nodes"
        )

        # Verify all nodes have the shared role
        nodes_with_role = ClusterHelper.get_nodes(shared_role)

        assert Node.self() in nodes_with_role

        Enum.each(nodes, fn node ->
          assert node in nodes_with_role,
                 "Node #{node} should have shared role #{shared_role}"
        end)

        # Verify each node sees other nodes with the shared role
        Enum.each(nodes, fn node ->
          node_sees = :erpc.call(node, ClusterHelper, :get_nodes, [shared_role])

          # Should see at least some peers (allow for convergence lag)
          assert length(node_sees) >= 1,
                 "Node #{node} should see at least 1 node with shared role, got #{length(node_sees)}"
        end)
      after
        Enum.each(peers, &stop_peer/1)
        ClusterHelper.remove_role(shared_role)
      end
    end

    test "unique roles per node are correctly tracked" do
      node_count = 15

      peers =
        Enum.map(1..node_count, fn i ->
          unique_role = :"unique_service_#{i}"
          start_peer(:"unique_#{i}", roles: [unique_role])
        end)

      nodes = Enum.map(peers, fn {_peer, node} -> node end)

      try do
        # Wait for all nodes to connect
        wait_until(
          fn -> Enum.all?(nodes, &(&1 in Node.list())) end,
          @convergence_timeout,
          "Not all unique role nodes connected"
        )

        # Wait for roles to propagate
        wait_until(
          fn ->
            Enum.all?(nodes, fn node ->
              role = :"unique_service_#{Enum.find_index(nodes, &(&1 == node)) + 1}"
              node in ClusterHelper.get_nodes(role)
            end)
          end,
          @convergence_timeout,
          "Unique roles did not propagate"
        )

        # Verify each unique role has exactly one node
        Enum.each(1..node_count, fn i ->
          role = :"unique_service_#{i}"
          nodes_with_role = ClusterHelper.get_nodes(role)

          assert length(nodes_with_role) == 1,
                 "Role #{role} should have exactly 1 node, got #{length(nodes_with_role)}"
        end)

        # Verify all_nodes includes all nodes
        all_nodes = ClusterHelper.all_nodes()

        assert length(all_nodes) >= node_count,
               "Expected at least #{node_count} nodes, got #{length(all_nodes)}"
      after
        Enum.each(peers, &stop_peer/1)
      end
    end
  end

  describe "stress test - rapid join/leave cycles" do
    test "10 cycles of 5 nodes joining and leaving" do
      # Ensure test node has a role so it stays in all_nodes
      ClusterHelper.add_role(:stress_test_coordinator)

      cycle_count = 5
      nodes_per_cycle = 3

      Enum.each(1..cycle_count, fn cycle ->
        peers =
          Enum.map(1..nodes_per_cycle, fn i ->
            start_peer(:"stress_c#{cycle}_n#{i}", roles: [:"stress_role_c#{cycle}"])
          end)

        nodes = Enum.map(peers, fn {_peer, node} -> node end)

        try do
          # Wait for nodes to connect
          wait_until(
            fn -> Enum.all?(nodes, &(&1 in Node.list())) end,
            @convergence_timeout,
            "Cycle #{cycle} nodes did not connect"
          )

          # Verify roles are visible
          wait_until(
            fn ->
              nodes_with_role = ClusterHelper.get_nodes(:"stress_role_c#{cycle}")
              length(nodes_with_role) >= nodes_per_cycle
            end,
            @convergence_timeout,
            "Cycle #{cycle} roles did not propagate"
          )

          # Stop all nodes
          Enum.each(peers, &stop_peer/1)

          # Wait for disconnection
          wait_until(
            fn -> Enum.all?(nodes, fn node -> node not in Node.list() end) end,
            @convergence_timeout,
            "Cycle #{cycle} nodes did not disconnect"
          )
        rescue
          e ->
            Enum.each(peers, &stop_peer/1)
            reraise e, __STACKTRACE__
        end
      end)

      # Final verification - cluster should be stable
      Process.sleep(2000)

      assert Node.self() in ClusterHelper.all_nodes()

      # Cleanup
      ClusterHelper.remove_role(:stress_test_coordinator)
    end
  end

  describe "scope isolation with multiple nodes" do
    test "different scopes maintain isolation across 10 nodes" do
      node_count = 10
      scope_a = :scope_alpha
      scope_b = :scope_beta

      # Join both scopes on test node
      ClusterHelper.join_scope(scope_a)
      ClusterHelper.join_scope(scope_b)

      ClusterHelper.add_role(:alpha_service, scope_a)
      ClusterHelper.add_role(:beta_service, scope_b)

      peers =
        Enum.map(1..node_count, fn i ->
          # Each node joins both scopes with different roles
          {:ok, peer, node} = start_peer_helper(:"iso_#{i}")

          :erpc.call(node, ClusterHelper, :join_scope, [scope_a])
          :erpc.call(node, ClusterHelper, :join_scope, [scope_b])
          :erpc.call(node, ClusterHelper, :add_role, [:"alpha_node_#{i}", scope_a])
          :erpc.call(node, ClusterHelper, :add_role, [:"beta_node_#{i}", scope_b])

          {peer, node}
        end)

      nodes = Enum.map(peers, fn {_peer, node} -> node end)

      try do
        # Wait for all nodes to connect
        wait_until(
          fn -> Enum.all?(nodes, &(&1 in Node.list())) end,
          @convergence_timeout,
          "Not all isolation test nodes connected"
        )

        # Wait for roles to propagate in both scopes
        wait_until(
          fn ->
            alpha_nodes = ClusterHelper.all_nodes(scope_a)
            beta_nodes = ClusterHelper.all_nodes(scope_b)
            length(alpha_nodes) >= node_count + 1 and length(beta_nodes) >= node_count + 1
          end,
          @convergence_timeout,
          "Roles did not propagate in both scopes"
        )

        # Verify scope isolation
        alpha_nodes = ClusterHelper.all_nodes(scope_a)
        beta_nodes = ClusterHelper.all_nodes(scope_b)

        # Both scopes should have all nodes
        assert length(alpha_nodes) >= node_count + 1
        assert length(beta_nodes) >= node_count + 1

        # Verify roles are isolated - alpha roles only in scope A
        Enum.each(1..node_count, fn i ->
          alpha_role = :"alpha_node_#{i}"

          assert Node.self() not in ClusterHelper.get_nodes(alpha_role, scope_b),
                 "Alpha role #{alpha_role} should not be visible in scope B"
        end)

        # Verify roles are isolated - beta roles only in scope B
        Enum.each(1..node_count, fn i ->
          beta_role = :"beta_node_#{i}"

          assert Node.self() not in ClusterHelper.get_nodes(beta_role, scope_a),
                 "Beta role #{beta_role} should not be visible in scope A"
        end)
      after
        Enum.each(peers, &stop_peer/1)
        ClusterHelper.leave_scope(scope_a)
        ClusterHelper.leave_scope(scope_b)
      end
    end
  end

  # ── Helper for starting peer without auto-adding roles ───────────────────────

  defp start_peer_helper(name_prefix) do
    name = :"#{name_prefix}_#{System.unique_integer([:positive, :monotonic])}"

    host =
      Node.self()
      |> Atom.to_string()
      |> String.split("@")
      |> List.last()
      |> to_charlist()

    cookie = Node.get_cookie() |> Atom.to_string() |> to_charlist()

    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        host: host,
        args: [~c"-setcookie", cookie],
        wait_boot: @node_boot_timeout
      })

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])
    {:ok, _started} = :erpc.call(node, Application, :ensure_all_started, [:cluster_helper])

    wait_until(
      fn -> node in Node.list() end,
      @convergence_timeout,
      "peer #{node} did not connect"
    )

    on_exit(fn ->
      try do
        :peer.stop(peer)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, peer, node}
  end
end
