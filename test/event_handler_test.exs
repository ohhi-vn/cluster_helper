
defmodule ClusterHelper.EventHandlerTest do
  @moduledoc """
  Unit tests for ClusterHelper.EventHandler dispatch logic.
  Uses a lightweight Agent to capture callback invocations — no NodeConfig
  process is needed.
  """
  use ExUnit.Case, async: false

  alias ClusterHelper.EventHandler

  # Collector that satisfies both optional callbacks.
  defmodule Collector do
    @behaviour ClusterHelper.EventHandler

    @impl true
    def on_role_added(node, role) do
      Agent.update(__MODULE__, fn events -> [{:role_added, node, role} | events] end)
    end

    @impl true
    def on_node_added(node) do
      Agent.update(__MODULE__, fn events -> [{:node_added, node} | events] end)
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
    # on_node_added/1 intentionally absent
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
  end

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
  end

  describe "optional callback — on_node_added missing" do
    test "on_role_added still fires when on_node_added is absent" do
      with_handler(PartialCollector, fn ->
        EventHandler.dispatch_role_added(Node.self(), :api)
        assert_received {:role_added, _, :api}
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
  # on_role_added/2 intentionally absent
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
    def on_node_added(node) do
      Agent.update(__MODULE__, fn {pid, evs} ->
        send(pid, {:callback, :node_added, node})
        {pid, [{:node_added, node} | evs]}
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
    ClusterHelper.get_my_roles() |> Enum.each(&ClusterHelper.remove_role/1)

    on_exit(fn ->
      Application.delete_env(:cluster_helper, :event_handler)
      ClusterHelper.get_my_roles() |> Enum.each(&ClusterHelper.remove_role/1)
    end)

    :ok
  end

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
    send(NodeConfig, {:new_roles, [:cache], remote})
    assert_receive {:callback, :role_added, ^remote, :cache}, 500
  end

  test "on_node_added fires when a new remote node is pulled for the first time" do
    remote = :"new_remote@127.0.0.1"
    send(NodeConfig, {:pull_new_node, remote, [:data]})
    assert_receive {:callback, :node_added, ^remote}, 500
  end

  test "on_node_added does NOT fire on periodic resync (pull_update_node)" do
    remote = :"known_remote@127.0.0.1"
    send(NodeConfig, {:pull_update_node, remote, [:data]})
    refute_receive {:callback, :node_added, ^remote}, 200
  end

  test "no duplicate on_role_added for idempotent add" do
    me = Node.self()
    ClusterHelper.add_role(:dedup)
    ClusterHelper.add_role(:dedup)
    assert_receive {:callback, :role_added, ^me, :dedup}, 500
    refute_receive {:callback, :role_added, ^me, :dedup}, 200
  end
end
