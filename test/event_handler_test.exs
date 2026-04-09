defmodule ClusterHelper.EventHandlerTest do
  @moduledoc """
  Unit tests for ClusterHelper.EventHandler dispatch logic.
  Uses a lightweight Agent to capture callback invocations — no NodeConfig
  process is needed.
  """
  use ExUnit.Case, async: false

  alias ClusterHelper.EventHandler

  # Collector that satisfies all four optional callbacks.
  defmodule Collector do
    @behaviour ClusterHelper.EventHandler

    @impl true
    def on_role_added(node, role) do
      Agent.update(__MODULE__, fn events -> [{:role_added, node, role} | events] end)
    end

    @impl true
    def on_role_removed(node, role) do
      Agent.update(__MODULE__, fn events -> [{:role_removed, node, role} | events] end)
    end

    @impl true
    def on_node_added(node) do
      Agent.update(__MODULE__, fn events -> [{:node_added, node} | events] end)
    end

    @impl true
    def on_node_removed(node) do
      Agent.update(__MODULE__, fn events -> [{:node_removed, node} | events] end)
    end

    def start(), do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def stop(), do: Agent.stop(__MODULE__)
    def events(), do: Agent.get(__MODULE__, &Enum.reverse/1)
    def reset(), do: Agent.update(__MODULE__, fn _ -> [] end)
  end

  # Implements only on_role_added — used to verify optional-callback safety.
  defmodule PartialCollector do
    @behaviour ClusterHelper.EventHandler

    @impl true
    def on_role_added(node, role), do: send(self(), {:role_added, node, role})
    # on_node_added/1, on_role_removed/2, on_node_removed/1 intentionally absent
  end

  # Implements only on_role_removed and on_node_removed — used to verify
  # that the "added" callbacks don't crash when absent.
  defmodule RemovedOnlyCollector do
    @behaviour ClusterHelper.EventHandler

    @impl true
    def on_role_removed(node, role), do: send(self(), {:role_removed, node, role})

    @impl true
    def on_node_removed(node), do: send(self(), {:node_removed, node})

    # on_role_added/2 and on_node_added/1 intentionally absent
  end

  setup_all do
    {:ok, _} = Collector.start()
    on_exit(&Collector.stop/0)
  end

  setup do
    Collector.reset()
    # Guarantee no stale handler from a previous test.
    Application.delete_env(:cluster_helper, :event_handler)
    :ok
  end

  # ── on_role_added ─────────────────────────────────────────────────────────────

  describe "dispatch_role_added/2" do
    test "calls on_role_added when handler is configured" do
      me = Node.self()

      with_handler(Collector, fn ->
        EventHandler.dispatch_role_added(me, :web)
        assert [{:role_added, ^me, :web}] = Collector.events()
      end)
    end

    test "passes non-atom roles correctly" do
      me = Node.self()

      with_handler(Collector, fn ->
        EventHandler.dispatch_role_added(me, "string_role")
        EventHandler.dispatch_role_added(me, {:shard, 3})
        EventHandler.dispatch_role_added(me, 7)

        events = Collector.events()
        assert Enum.any?(events, &match?({:role_added, _, "string_role"}, &1))
        assert Enum.any?(events, &match?({:role_added, _, {:shard, 3}}, &1))
        assert Enum.any?(events, &match?({:role_added, _, 7}, &1))
      end)
    end

    test "is a no-op when no handler is configured" do
      Application.delete_env(:cluster_helper, :event_handler)
      assert :ok = EventHandler.dispatch_role_added(Node.self(), :web)
    end

    test "does not raise when handler implements only on_node_added" do
      with_handler(NodeOnlyCollector, fn ->
        assert :ok = EventHandler.dispatch_role_added(Node.self(), :web)
      end)
    end

    test "does not raise when handler implements only on_role_removed" do
      with_handler(RemovedOnlyCollector, fn ->
        assert :ok = EventHandler.dispatch_role_added(Node.self(), :web)
      end)
    end
  end

  # ── on_node_added ─────────────────────────────────────────────────────────────

  describe "dispatch_node_added/1" do
    test "calls on_node_added when handler is configured" do
      remote = :"remote@127.0.0.1"

      with_handler(Collector, fn ->
        EventHandler.dispatch_node_added(remote)
        assert [{:node_added, ^remote}] = Collector.events()
      end)
    end

    test "is a no-op when no handler is configured" do
      Application.delete_env(:cluster_helper, :event_handler)
      assert :ok = EventHandler.dispatch_node_added(:"remote@127.0.0.1")
    end

    test "does not raise when handler implements only on_role_added" do
      with_handler(PartialCollector, fn ->
        assert :ok = EventHandler.dispatch_node_added(:"remote@127.0.0.1")
      end)
    end

    test "does not raise when handler implements only on_node_removed" do
      with_handler(RemovedOnlyCollector, fn ->
        assert :ok = EventHandler.dispatch_node_added(:"remote@127.0.0.1")
      end)
    end
  end

  # ── on_role_removed ───────────────────────────────────────────────────────────

  describe "dispatch_role_removed/2" do
    test "calls on_role_removed when handler is configured" do
      me = Node.self()

      with_handler(Collector, fn ->
        EventHandler.dispatch_role_removed(me, :web)
        assert [{:role_removed, ^me, :web}] = Collector.events()
      end)
    end

    test "passes non-atom roles correctly" do
      me = Node.self()

      with_handler(Collector, fn ->
        EventHandler.dispatch_role_removed(me, "string_role")
        EventHandler.dispatch_role_removed(me, {:shard, 3})
        EventHandler.dispatch_role_removed(me, 7)

        events = Collector.events()
        assert Enum.any?(events, &match?({:role_removed, _, "string_role"}, &1))
        assert Enum.any?(events, &match?({:role_removed, _, {:shard, 3}}, &1))
        assert Enum.any?(events, &match?({:role_removed, _, 7}, &1))
      end)
    end

    test "is a no-op when no handler is configured" do
      Application.delete_env(:cluster_helper, :event_handler)
      assert :ok = EventHandler.dispatch_role_removed(Node.self(), :web)
    end

    test "does not raise when handler implements only on_role_added" do
      with_handler(PartialCollector, fn ->
        assert :ok = EventHandler.dispatch_role_removed(Node.self(), :web)
      end)
    end

    test "does not raise when handler implements only on_node_added" do
      with_handler(NodeOnlyCollector, fn ->
        assert :ok = EventHandler.dispatch_role_removed(Node.self(), :web)
      end)
    end
  end

  # ── on_node_removed ───────────────────────────────────────────────────────────

  describe "dispatch_node_removed/1" do
    test "calls on_node_removed when handler is configured" do
      remote = :"remote@127.0.0.1"

      with_handler(Collector, fn ->
        EventHandler.dispatch_node_removed(remote)
        assert [{:node_removed, ^remote}] = Collector.events()
      end)
    end

    test "is a no-op when no handler is configured" do
      Application.delete_env(:cluster_helper, :event_handler)
      assert :ok = EventHandler.dispatch_node_removed(:"remote@127.0.0.1")
    end

    test "does not raise when handler implements only on_role_added" do
      with_handler(PartialCollector, fn ->
        assert :ok = EventHandler.dispatch_node_removed(:"remote@127.0.0.1")
      end)
    end

    test "does not raise when handler implements only on_role_removed" do
      with_handler(RemovedOnlyCollector, fn ->
        # RemovedOnlyCollector has on_role_removed but not on_node_removed
        assert :ok = EventHandler.dispatch_node_removed(:"remote@127.0.0.1")
      end)
    end
  end

  # ── Optional callback safety ──────────────────────────────────────────────────

  describe "optional callback — on_node_added missing" do
    test "on_role_added still fires when on_node_added is absent" do
      with_handler(PartialCollector, fn ->
        EventHandler.dispatch_role_added(Node.self(), :api)
        assert_received {:role_added, _, :api}
      end)
    end
  end

  describe "optional callback — on_role_removed missing" do
    test "on_role_added still fires when on_role_removed is absent" do
      with_handler(PartialCollector, fn ->
        EventHandler.dispatch_role_added(Node.self(), :api)
        assert_received {:role_added, _, :api}
      end)
    end

    test "dispatch_role_removed does not raise when on_role_removed is absent" do
      with_handler(PartialCollector, fn ->
        assert :ok = EventHandler.dispatch_role_removed(Node.self(), :api)
      end)
    end
  end

  describe "optional callback — on_node_removed missing" do
    test "dispatch_node_removed does not raise when on_node_removed is absent" do
      with_handler(PartialCollector, fn ->
        assert :ok = EventHandler.dispatch_node_removed(:"remote@127.0.0.1")
      end)
    end
  end

  # ── Helpers ──────────────────────────────────────────────────────────────────

  defp with_handler(mod, fun) do
    Application.put_env(:cluster_helper, :event_handler, mod)

    try do
      fun.()
    after
      Application.delete_env(:cluster_helper, :event_handler)
    end
  end
end

# Defined at the top level so it is a real compiled module —
# function_exported?/3 requires this.
defmodule NodeOnlyCollector do
  @behaviour ClusterHelper.EventHandler

  @impl true
  def on_node_added(_node), do: :ok
  # on_role_added/2, on_role_removed/2, on_node_removed/1 intentionally absent
end

defmodule ClusterHelper.NodeConfigCallbackIntegrationTest do
  @moduledoc """
  Integration tests that verify callbacks fire through the full NodeConfig
  message-handling path.

  The supervised NodeConfig instance is reused — we never stop it.
  IntegrationCollector is started once for the whole suite and reset
  between tests.
  """
  use ExUnit.Case, async: false

  alias ClusterHelper.NodeConfig

  defmodule IntegrationCollector do
    @behaviour ClusterHelper.EventHandler

    # test_pid is stored so callbacks can send messages for assert_receive.
    def start(test_pid),
      do: Agent.start_link(fn -> {test_pid, []} end, name: __MODULE__)

    def stop do
      try do
        Agent.stop(__MODULE__)
      catch
        :exit, _ -> :ok
      end
    end

    def reset(test_pid),
      do: Agent.update(__MODULE__, fn _ -> {test_pid, []} end)

    def events(),
      do: Agent.get(__MODULE__, fn {_, evs} -> Enum.reverse(evs) end)

    @impl true
    def on_role_added(node, role) do
      Agent.update(__MODULE__, fn {pid, evs} ->
        send(pid, {:callback, :role_added, node, role})
        {pid, [{:role_added, node, role} | evs]}
      end)
    end

    @impl true
    def on_role_removed(node, role) do
      Agent.update(__MODULE__, fn {pid, evs} ->
        send(pid, {:callback, :role_removed, node, role})
        {pid, [{:role_removed, node, role} | evs]}
      end)
    end

    @impl true
    def on_node_added(node) do
      Agent.update(__MODULE__, fn {pid, evs} ->
        send(pid, {:callback, :node_added, node})
        {pid, [{:node_added, node} | evs]}
      end)
    end

    @impl true
    def on_node_removed(node) do
      Agent.update(__MODULE__, fn {pid, evs} ->
        send(pid, {:callback, :node_removed, node})
        {pid, [{:node_removed, node} | evs]}
      end)
    end
  end

  # Start the collector once for the entire suite.
  setup_all do
    {:ok, _} = IntegrationCollector.start(self())
    on_exit(&IntegrationCollector.stop/0)
    :ok
  end

  setup do
    # Point each test's messages at the current test process.
    IntegrationCollector.reset(self())
    Application.put_env(:cluster_helper, :event_handler, IntegrationCollector)

    # Clean up any roles left by the previous test.
    # Wait for GenServer to be ready (may be busy processing async tasks from prior tests).
    wait_for_gen_server()
    ClusterHelper.get_my_roles() |> Enum.each(&ClusterHelper.remove_role/1)

    on_exit(fn ->
      Application.delete_env(:cluster_helper, :event_handler)
      ClusterHelper.get_my_roles() |> Enum.each(&ClusterHelper.remove_role/1)
    end)

    :ok
  end

  # ── on_role_added integration tests ──────────────────────────────────────────

  describe "on_role_added integration" do
    test "on_role_added fires when a local role is added" do
      me = Node.self()
      ClusterHelper.add_role(:worker)
      assert_receive {:callback, :role_added, ^me, :worker}, 500
    end

    test "on_role_added fires for each role in a batch add" do
      me = Node.self()
      ClusterHelper.add_roles([:a, :b, :c])
      assert_receive {:callback, :role_added, ^me, :a}, 500
      assert_receive {:callback, :role_added, ^me, :b}, 500
      assert_receive {:callback, :role_added, ^me, :c}, 500
    end

    test "on_role_added fires for non-atom roles" do
      me = Node.self()
      ClusterHelper.add_roles(["str", {:t, 1}, 42])
      assert_receive {:callback, :role_added, ^me, "str"}, 500
      assert_receive {:callback, :role_added, ^me, {:t, 1}}, 500
      assert_receive {:callback, :role_added, ^me, 42}, 500
    end

    test "on_role_added fires when a remote node's roles are received via pub/sub" do
      remote = :"fake_remote@127.0.0.1"
      send(NodeConfig, {:new_roles, ClusterHelper, [:cache], remote})
      assert_receive {:callback, :role_added, ^remote, :cache}, 500
    end

    test "no duplicate on_role_added for idempotent add" do
      me = Node.self()
      ClusterHelper.add_role(:dedup)
      ClusterHelper.add_role(:dedup)
      assert_receive {:callback, :role_added, ^me, :dedup}, 500
      refute_receive {:callback, :role_added, ^me, :dedup}, 200
    end
  end

  # ── on_role_removed integration tests ─────────────────────────────────────────

  describe "on_role_removed integration" do
    test "on_role_removed fires when a local role is removed" do
      me = Node.self()
      ClusterHelper.add_role(:to_remove)
      # Flush the add callback
      assert_receive {:callback, :role_added, ^me, :to_remove}, 500

      ClusterHelper.remove_role(:to_remove)
      assert_receive {:callback, :role_removed, ^me, :to_remove}, 500
    end

    test "on_role_removed fires for each role in a batch remove" do
      me = Node.self()
      ClusterHelper.add_roles([:x, :y, :z])
      # Flush add callbacks
      assert_receive {:callback, :role_added, ^me, :x}, 500
      assert_receive {:callback, :role_added, ^me, :y}, 500
      assert_receive {:callback, :role_added, ^me, :z}, 500

      ClusterHelper.remove_roles([:x, :y])
      assert_receive {:callback, :role_removed, ^me, :x}, 500
      assert_receive {:callback, :role_removed, ^me, :y}, 500
    end

    test "on_role_removed fires for non-atom roles" do
      me = Node.self()
      ClusterHelper.add_roles(["str_role", {:tuple_role, 1}])
      # Flush add callbacks
      assert_receive {:callback, :role_added, ^me, "str_role"}, 500
      assert_receive {:callback, :role_added, ^me, {:tuple_role, 1}}, 500

      ClusterHelper.remove_roles(["str_role", {:tuple_role, 1}])
      assert_receive {:callback, :role_removed, ^me, "str_role"}, 500
      assert_receive {:callback, :role_removed, ^me, {:tuple_role, 1}}, 500
    end

    test "on_role_removed fires when a remote node's roles are removed via pub/sub" do
      remote = :"fake_remote@127.0.0.1"
      # First add roles for the remote node
      send(NodeConfig, {:new_roles, ClusterHelper, [:cache, :api], remote})
      assert_receive {:callback, :role_added, ^remote, :cache}, 500
      assert_receive {:callback, :role_added, ^remote, :api}, 500

      # Now remove one role via pub/sub message
      send(NodeConfig, {:remove_roles, ClusterHelper, [:cache], remote})
      assert_receive {:callback, :role_removed, ^remote, :cache}, 500
    end

    test "on_role_removed does NOT fire when removing a non-existent role" do
      me = Node.self()
      ClusterHelper.remove_role(:nonexistent)
      refute_receive {:callback, :role_removed, ^me, :nonexistent}, 200
    end
  end

  # ── on_node_added integration tests ───────────────────────────────────────────

  describe "on_node_added integration" do
    test "on_node_added fires when a new remote node is pulled for the first time" do
      remote = :"new_remote@127.0.0.1"
      send(NodeConfig, {:pull_new_node, ClusterHelper, remote, [:data]})
      assert_receive {:callback, :node_added, ^remote}, 500
    end

    test "on_node_added does NOT fire on periodic resync (pull_update_node)" do
      remote = :"known_remote@127.0.0.1"
      send(NodeConfig, {:pull_update_node, ClusterHelper, remote, [:data]})
      refute_receive {:callback, :node_added, ^remote}, 200
    end

    test "on_node_added fires with generation metadata" do
      remote = :"gen_remote@127.0.0.1"
      send(NodeConfig, {:pull_new_node, ClusterHelper, remote, [:data], 5})
      assert_receive {:callback, :node_added, ^remote}, 500
    end
  end

  # ── on_node_removed integration tests ─────────────────────────────────────────

  describe "on_node_removed integration" do
    test "on_node_removed fires on nodedown" do
      remote = :"down_remote@127.0.0.1"
      # First register the node so it exists in ETS
      send(NodeConfig, {:new_roles, ClusterHelper, [:worker], remote})
      assert_receive {:callback, :role_added, ^remote, :worker}, 500

      # Simulate nodedown
      send(NodeConfig, {:nodedown, remote, []})
      assert_receive {:callback, :node_removed, ^remote}, 500
    end

    test "on_node_removed fires even if the node had no roles" do
      remote = :"empty_remote@127.0.0.1"
      # Simulate nodedown for a node that was never seen before
      send(NodeConfig, {:nodedown, remote, []})
      assert_receive {:callback, :node_removed, ^remote}, 500
    end

    test "on_node_removed fires and on_role_removed fires for each role on nodedown" do
      remote = :"multi_role_remote@127.0.0.1"
      # Register the node with multiple roles
      send(NodeConfig, {:new_roles, ClusterHelper, [:web, :api, :cache], remote})
      assert_receive {:callback, :role_added, ^remote, :web}, 500
      assert_receive {:callback, :role_added, ^remote, :api}, 500
      assert_receive {:callback, :role_added, ^remote, :cache}, 500

      # Simulate nodedown — should fire on_role_removed for each role AND on_node_removed
      send(NodeConfig, {:nodedown, remote, []})
      assert_receive {:callback, :role_removed, ^remote, :web}, 500
      assert_receive {:callback, :role_removed, ^remote, :api}, 500
      assert_receive {:callback, :role_removed, ^remote, :cache}, 500
      assert_receive {:callback, :node_removed, ^remote}, 500
    end
  end

  # ── Generation tracking integration tests ─────────────────────────────────────

  describe "generation tracking" do
    test "generation is a non-negative integer for the default scope" do
      # Generation accumulates across tests (shared GenServer), so we only
      # verify it is a non-negative integer, not a specific value.
      gen = ClusterHelper.NodeConfig.__get_generation__(ClusterHelper)
      assert is_integer(gen) and gen >= 0
    end

    test "generation increments when roles are added" do
      initial_gen = ClusterHelper.NodeConfig.__get_generation__(ClusterHelper)

      ClusterHelper.add_role(:gen_test)
      new_gen = ClusterHelper.NodeConfig.__get_generation__(ClusterHelper)

      assert new_gen > initial_gen

      ClusterHelper.remove_role(:gen_test)
    end

    test "generation increments when roles are removed" do
      ClusterHelper.add_role(:gen_test2)
      gen_after_add = ClusterHelper.NodeConfig.__get_generation__(ClusterHelper)

      ClusterHelper.remove_role(:gen_test2)
      gen_after_remove = ClusterHelper.NodeConfig.__get_generation__(ClusterHelper)

      assert gen_after_remove > gen_after_add
    end

    test "generation does not increment when adding a duplicate role" do
      ClusterHelper.add_role(:dup_gen)
      gen1 = ClusterHelper.NodeConfig.__get_generation__(ClusterHelper)

      ClusterHelper.add_role(:dup_gen)
      gen2 = ClusterHelper.NodeConfig.__get_generation__(ClusterHelper)

      assert gen2 == gen1

      ClusterHelper.remove_role(:dup_gen)
    end

    test "__get_scopes__/0 returns the default scope" do
      scopes = ClusterHelper.NodeConfig.__get_scopes__()
      assert ClusterHelper in scopes
    end

    test "__get_scopes__/0 includes joined scopes" do
      scope = :"test_scope_#{System.unique_integer([:positive])}"
      ClusterHelper.join_scope(scope)

      scopes = ClusterHelper.NodeConfig.__get_scopes__()
      assert scope in scopes

      ClusterHelper.leave_scope(scope)
    end
  end

  # ── Helpers ──────────────────────────────────────────────────────────────────

  # Retry calling get_my_roles until the GenServer is ready to respond.
  defp wait_for_gen_server(retries \\ 20)
  defp wait_for_gen_server(0), do: :ok

  defp wait_for_gen_server(retries) do
    try do
      ClusterHelper.get_my_roles()
      :ok
    catch
      :exit, _ ->
        Process.sleep(100)
        wait_for_gen_server(retries - 1)
    end
  end
end
