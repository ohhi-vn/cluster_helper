# test/cluster_helper_cluster_test.exs
#
# Real multi-node integration tests.
#
# These tests spin up actual Erlang peer nodes in-process using the OTP :peer
# module (requires OTP 25+). Each test starts one or more isolated peer nodes,
# loads ClusterHelper onto them, and verifies role propagation over real
# distributed Erlang.
#
# Running
# ───────
#   # Run only cluster tests (requires a named node):
#   mix test --only cluster --name test@127.0.0.1
#
#   # Exclude cluster tests (default CI / fast feedback loop):
#   mix test --exclude cluster
#
# The test node must be started with --name (or --sname) so that peer nodes
# can connect to it. The setup_all block handles this automatically when
# running via plain `mix test`, but --name is recommended for reliability.

defmodule ClusterHelper.ClusterTest do
  use ExUnit.Case, async: false

  @moduletag :cluster

  # How long to wait for async role propagation before failing a test.
  @propagation_timeout 5_000
  # How long to wait for a full pull cycle when testing node departure or
  # cross-peer role convergence. Must exceed pull_interval (default 7 000 ms).
  @departure_timeout 15_000
  @poll_interval 100

  # ── Suite-level setup ─────────────────────────────────────────────────────

  setup_all do
    # Require OTP 27+ — :peer.start/1 returns a 3-tuple {pid, node} only from
    # OTP 27 onward. Earlier releases had a different API (get_node/1).
    otp = :erlang.system_info(:otp_release) |> List.to_integer()

    if otp < 27 do
      raise "Cluster tests require OTP 27+. Running OTP #{otp}."
    end

    # Verify the test runner is a distributed node.
    # When invoked via `mix test.cluster` / `mix test.all` this is always true
    # because the alias passes `--name test@127.0.0.1` to the elixir binary.
    unless Node.alive?() do
      raise """
      Cluster tests require a named node.
      Run them with:

          mix test.cluster

      or manually (use a unique name to avoid epmd conflicts):

          elixir --name test_$@127.0.0.1 -S mix test --only cluster
      """
    end

    :ok
  end

  # ── Per-test setup ────────────────────────────────────────────────────────

  setup do
    # Always start each test with a clean local role list.
    ClusterHelper.get_my_roles() |> Enum.each(&ClusterHelper.remove_role/1)
    :ok
  end

  # ═══════════════════════════════════════════════════════════════════════════
  # Initial sync — roles present before / at peer startup
  # ═══════════════════════════════════════════════════════════════════════════

  describe "initial sync on node join" do
    test "newly started peer discovers the test node's existing roles" do
      ClusterHelper.add_role(:coordinator)

      {_peer, peer_node} = start_peer(:alpha)

      wait_until(
        fn ->
          Node.self() in :erpc.call(peer_node, ClusterHelper, :get_nodes, [:coordinator])
        end,
        @propagation_timeout,
        "peer #{peer_node} should discover :coordinator role on test node"
      )
    end

    test "test node discovers a peer's startup roles" do
      {_peer, peer_node} = start_peer(:beta, roles: [:worker])

      wait_until(
        fn -> peer_node in ClusterHelper.get_nodes(:worker) end,
        @propagation_timeout,
        "test node should see peer's :worker startup role"
      )
    end

    test "get_roles/1 reflects multiple startup roles from a remote node" do
      {_peer, peer_node} = start_peer(:gamma, roles: [:api, :cache])

      wait_until(
        fn ->
          roles = ClusterHelper.get_roles(peer_node)
          :api in roles and :cache in roles
        end,
        @propagation_timeout,
        "test node should see both :api and :cache from peer"
      )
    end

    test "all_nodes/0 includes the peer once it registers at least one role" do
      {_peer, peer_node} = start_peer(:delta, roles: [:edge])

      wait_until(
        fn -> peer_node in ClusterHelper.all_nodes() end,
        @propagation_timeout,
        "peer should appear in all_nodes/0 after joining"
      )
    end
  end

  # ═══════════════════════════════════════════════════════════════════════════
  # Live propagation — role changes after the cluster is formed
  # ═══════════════════════════════════════════════════════════════════════════

  describe "live role changes propagate cluster-wide" do
    test "add_role/1 on a peer is immediately visible on the test node" do
      {_peer, peer_node} = start_peer(:epsilon)
      # all_nodes/0 only lists nodes with at least one role — use :syn instead.
      scope = Application.get_env(:cluster_helper, :scope, ClusterHelper)
      wait_until(fn ->
        :syn.members(scope, :all_nodes) |> Enum.any?(fn {_, n} -> n == peer_node end)
      end)

      :erpc.call(peer_node, ClusterHelper, :add_role, [:live_added])

      wait_until(
        fn -> peer_node in ClusterHelper.get_nodes(:live_added) end,
        @propagation_timeout,
        "test node should see :live_added role added on peer"
      )
    end

    test "remove_role/1 on a peer is immediately reflected on the test node" do
      {_peer, peer_node} = start_peer(:zeta, roles: [:to_remove])
      wait_until(fn -> peer_node in ClusterHelper.get_nodes(:to_remove) end)

      :erpc.call(peer_node, ClusterHelper, :remove_role, [:to_remove])

      wait_until(
        fn -> peer_node not in ClusterHelper.get_nodes(:to_remove) end,
        @propagation_timeout,
        ":to_remove should be evicted from test node after peer removes it"
      )
    end

    test "add_roles/1 propagates all new roles in a single pub/sub event" do
      {_peer, peer_node} = start_peer(:eta)
      scope = Application.get_env(:cluster_helper, :scope, ClusterHelper)
      wait_until(fn ->
        :syn.members(scope, :all_nodes) |> Enum.any?(fn {_, n} -> n == peer_node end)
      end)

      :erpc.call(peer_node, ClusterHelper, :add_roles, [[:svc_a, :svc_b, :svc_c]])

      wait_until(
        fn ->
          peer_node in ClusterHelper.get_nodes(:svc_a) and
            peer_node in ClusterHelper.get_nodes(:svc_b) and
            peer_node in ClusterHelper.get_nodes(:svc_c)
        end,
        @propagation_timeout,
        "all three roles added via add_roles/1 should appear on test node"
      )
    end

    test "remove_roles/1 propagates all removals while preserving remaining roles" do
      {_peer, peer_node} = start_peer(:theta, roles: [:r1, :r2, :r3])

      wait_until(fn ->
        peer_node in ClusterHelper.get_nodes(:r1) and
          peer_node in ClusterHelper.get_nodes(:r2) and
          peer_node in ClusterHelper.get_nodes(:r3)
      end)

      :erpc.call(peer_node, ClusterHelper, :remove_roles, [[:r1, :r2]])

      wait_until(
        fn ->
          peer_node not in ClusterHelper.get_nodes(:r1) and
            peer_node not in ClusterHelper.get_nodes(:r2) and
            peer_node in ClusterHelper.get_nodes(:r3)
        end,
        @propagation_timeout,
        ":r1 and :r2 should be gone; :r3 must remain"
      )
    end

    test "role added on test node is visible on the peer" do
      {_peer, peer_node} = start_peer(:iota)
      scope = Application.get_env(:cluster_helper, :scope, ClusterHelper)
      wait_until(fn ->
        :syn.members(scope, :all_nodes) |> Enum.any?(fn {_, n} -> n == peer_node end)
      end)

      ClusterHelper.add_role(:test_node_role)

      wait_until(
        fn ->
          Node.self() in :erpc.call(peer_node, ClusterHelper, :get_nodes, [:test_node_role])
        end,
        @propagation_timeout,
        "peer should see role added on the test node"
      )
    end
  end

  # ═══════════════════════════════════════════════════════════════════════════
  # Node departure
  # ═══════════════════════════════════════════════════════════════════════════

  describe "node departure" do
    test "stopping a peer evicts all its roles from the cluster after the pull cycle" do
      {peer, peer_node} = start_peer(:kappa, roles: [:ephemeral])
      wait_until(fn -> peer_node in ClusterHelper.get_nodes(:ephemeral) end)

      # Stop the peer asynchronously so we don't block the test process.
      Task.start(fn -> :peer.stop(peer) end)

      wait_until(
        fn ->
          peer_node not in ClusterHelper.all_nodes() and
            ClusterHelper.get_nodes(:ephemeral) == []
        end,
        @departure_timeout,
        "stopped peer and its roles should be evicted after the next pull cycle"
      )
    end

    test "other roles in the cluster are unaffected when one node leaves" do
      ClusterHelper.add_role(:stable_role)
      {peer, peer_node} = start_peer(:lambda, roles: [:leaving_role])

      wait_until(fn ->
        peer_node in ClusterHelper.get_nodes(:leaving_role) and
          Node.self() in ClusterHelper.get_nodes(:stable_role)
      end)

      Task.start(fn -> :peer.stop(peer) end)

      wait_until(
        fn ->
          peer_node not in ClusterHelper.all_nodes() and
            Node.self() in ClusterHelper.get_nodes(:stable_role)
        end,
        @departure_timeout,
        ":stable_role on test node must survive after peer leaves"
      )
    end
  end

  # ═══════════════════════════════════════════════════════════════════════════
  # Multi-node cluster convergence
  # ═══════════════════════════════════════════════════════════════════════════

  describe "three-node cluster" do
    test "all nodes converge on a consistent cluster-wide role view" do
      ClusterHelper.add_role(:node0_role)
      {_p1, node1} = start_peer(:mu, roles: [:node1_role])
      {_p2, node2} = start_peer(:nu, roles: [:node2_role])

      # Connect the two peers to each other; they already connect to the test
      # node automatically during start_peer (via the host/cookie args).
      :erpc.call(node1, Node, :connect, [node2])

      scope = Application.get_env(:cluster_helper, :scope, ClusterHelper)

      # Wait until all three nodes reach full three-way :syn connectivity
      # before asserting role visibility.
      wait_until(
        fn ->
          node2_members =
            :erpc.call(node2, :syn, :members, [scope, :all_nodes])
            |> Enum.map(fn {_pid, n} -> n end)

          node1_members =
            :erpc.call(node1, :syn, :members, [scope, :all_nodes])
            |> Enum.map(fn {_pid, n} -> n end)

          node1 in node2_members and node2 in node1_members
        end,
        @propagation_timeout,
        "node1 and node2 did not reach mutual syn visibility"
      )

      wait_until(
        fn ->
          # Test node sees all peers' roles.
          local_ok =
            node1 in ClusterHelper.get_nodes(:node1_role) and
              node2 in ClusterHelper.get_nodes(:node2_role)

          # node1 sees test node and node2.
          node1_ok =
            :erpc.call(node1, ClusterHelper, :get_nodes, [:node0_role]) != [] and
              :erpc.call(node1, ClusterHelper, :get_nodes, [:node2_role]) != []

          # node2 sees test node and node1.
          node2_ok =
            :erpc.call(node2, ClusterHelper, :get_nodes, [:node0_role]) != [] and
              :erpc.call(node2, ClusterHelper, :get_nodes, [:node1_role]) != []

          local_ok and node1_ok and node2_ok
        end,
        # Must exceed the pull_interval (default 7 000 ms) so node1 and node2
        # have time to exchange roles after their :syn connection is established.
        15_000,
        "all three nodes must converge on a consistent role view"
      )
    end

    test "a role added dynamically on one peer is seen by all other nodes" do
      {_p1, node1} = start_peer(:xi)
      {_p2, node2} = start_peer(:omicron)

      :erpc.call(node1, Node, :connect, [node2])

      scope = Application.get_env(:cluster_helper, :scope, ClusterHelper)

      wait_until(
        fn ->
          node2_members =
            :erpc.call(node2, :syn, :members, [scope, :all_nodes])
            |> Enum.map(fn {_pid, n} -> n end)

          node1_members =
            :erpc.call(node1, :syn, :members, [scope, :all_nodes])
            |> Enum.map(fn {_pid, n} -> n end)

          node1 in node2_members and node2 in node1_members
        end,
        @propagation_timeout,
        "node1 and node2 did not reach mutual syn visibility"
      )

      :erpc.call(node1, ClusterHelper, :add_role, [:dynamic])

      wait_until(
        fn ->
          node1 in ClusterHelper.get_nodes(:dynamic) and
            node1 in :erpc.call(node2, ClusterHelper, :get_nodes, [:dynamic])
        end,
        @propagation_timeout,
        "dynamic role added on node1 should reach test node AND node2"
      )
    end
  end

  # ═══════════════════════════════════════════════════════════════════════════
  # Private helpers
  # ═══════════════════════════════════════════════════════════════════════════

  # Start a fresh peer node, load ClusterHelper onto it, and optionally
  # assign initial roles. Registers an on_exit cleanup automatically.
  #
  # Options:
  #   roles: [atom()] — roles to set on the peer right after startup
  #
  @spec start_peer(atom(), keyword()) :: {:peer.peer(), node()}
  defp start_peer(name_prefix, opts \\ []) do
    # Use a monotonic unique integer so test runs never collide on node names.
    name = :"#{name_prefix}_#{System.unique_integer([:positive, :monotonic])}"
    roles = Keyword.get(opts, :roles, [])

    # Derive host and cookie from the running test node so the peer can
    # connect back successfully.
    # Without an explicit host, :peer may fail DNS resolution.
    # Without a matching cookie, the distribution handshake times out.
    host =
      Node.self()
      |> Atom.to_string()
      |> String.split("@")
      |> List.last()
      |> to_charlist()

    cookie = Node.get_cookie() |> Atom.to_string() |> to_charlist()

    # :peer.start/1 returns {pid, node} in OTP 25-26 with get_node/1,
    # and {pid, node} directly as a 3-tuple in OTP 27+.
    # We match the 3-tuple form which works on OTP 27+.
    {:ok, peer, node} =
      :peer.start(%{
        name: name,
        host: host,
        args: [~c"-setcookie", cookie],
        wait_boot: 30_000
      })

    # Load local code paths onto the peer so ClusterHelper modules are found.
    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])

    # Start ClusterHelper (this also starts :syn transitively).
    {:ok, _started} = :erpc.call(node, Application, :ensure_all_started, [:cluster_helper])

    # Wait until the peer's NodeConfig has completed handle_continue and joined
    # the :all_nodes syn group. This is a reliable signal that the peer is fully
    # up and has performed its initial pull — much safer than a fixed sleep.
    scope = Application.get_env(:cluster_helper, :scope, ClusterHelper)

    wait_until(
      fn ->
        :syn.members(scope, :all_nodes)
        |> Enum.any?(fn {_pid, member} -> member == node end)
      end,
      10_000,
      "peer #{node} did not join :syn :all_nodes group within 10 s"
    )

    # Assign initial roles after the peer is confirmed to be in the syn scope.
    unless roles == [] do
      :erpc.call(node, ClusterHelper, :add_roles, [roles])
    end

    # Unconditionally clean up the peer when the test exits — tolerates the
    # case where the peer was already stopped by the test itself.
    on_exit(fn ->
      try do
        :peer.stop(peer)
      catch
        _, _ -> :ok
      end

      # Also remove any stale ETS entries on the test node in case the pull
      # cycle hasn't fired yet.
      ClusterHelper.get_my_roles() |> Enum.each(&ClusterHelper.remove_role/1)
    end)

    {peer, node}
  end

  # Poll `condition` every @poll_interval ms until it returns truthy.
  # Calls ExUnit.Assertions.flunk/1 with `msg` on timeout so the test
  # failure output is descriptive.
  @spec wait_until((() -> boolean()), non_neg_integer(), String.t()) :: :ok
  defp wait_until(condition, timeout \\ @propagation_timeout, msg \\ "condition never became true") do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_poll(condition, deadline, msg)
  end

  defp do_poll(condition, deadline, msg) do
    if condition.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        ExUnit.Assertions.flunk(msg)
      else
        Process.sleep(@poll_interval)
        do_poll(condition, deadline, msg)
      end
    end
  end
end
