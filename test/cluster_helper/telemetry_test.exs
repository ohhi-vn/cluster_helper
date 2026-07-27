defmodule ClusterHelper.TelemetryTest do
  use ExUnit.Case, async: false

  alias ClusterHelper.Telemetry

  describe "event emission" do
    setup do
      ClusterHelper.get_my_roles() |> Enum.each(&ClusterHelper.remove_role/1)
      test_pid = self()

      :telemetry.attach_many(
        "cluster-helper-test",
        Telemetry.event_names(),
        fn event_name, measurements, metadata, _ ->
          send(test_pid, {:telemetry_event, event_name, measurements, metadata})
        end,
        nil
      )

      on_exit(fn ->
        :telemetry.detach("cluster-helper-test")
      end)

      :ok
    end

    test "emits [:cluster_helper, :role, :add] on add_role" do
      ClusterHelper.add_role(:telemetry_add_test)

      assert_receive {:telemetry_event, [:cluster_helper, :role, :add], %{count: 1},
                      %{scope: _, roles: [:telemetry_add_test], node: _}}
    end

    test "emits [:cluster_helper, :role, :remove] on remove_role" do
      ClusterHelper.add_role(:telemetry_remove_test)
      ClusterHelper.remove_role(:telemetry_remove_test)

      assert_receive {:telemetry_event, [:cluster_helper, :role, :remove], %{count: 1},
                      %{scope: _, roles: [:telemetry_remove_test], node: _}}
    end

    test "does not emit role events for no-op operations" do
      ClusterHelper.remove_role(:never_added)

      refute_receive {:telemetry_event, [:cluster_helper, :role, :remove], _, _}
    end

    test "emits {:telemetry_event, [:cluster_helper, :node, :up], ...} on nodeup" do
      remote = :"telemetry_nodeup@127.0.0.1"
      ClusterHelper.NodeConfig.__get_scopes__()

      send(ClusterHelper.NodeConfig, {:nodeup, remote, []})
      assert_receive {:telemetry_event, [:cluster_helper, :node, :up], %{},
                      %{node: ^remote, scopes: _}}, 500
    end

    test "emits {:telemetry_event, [:cluster_helper, :node, :down], ...} on nodedown" do
      remote = :"telemetry_nodedown@127.0.0.1"

      send(ClusterHelper.NodeConfig, {:nodedown, remote, []})
      assert_receive {:telemetry_event, [:cluster_helper, :node, :down], %{},
                      %{node: ^remote}}, 500
    end
  end

  describe "event_names/0" do
    test "returns all event names" do
      names = Telemetry.event_names()

      assert [:cluster_helper, :startup] in names
      assert [:cluster_helper, :role, :add] in names
      assert [:cluster_helper, :role, :remove] in names
      assert [:cluster_helper, :sync, :pull] in names
      assert [:cluster_helper, :node, :up] in names
      assert [:cluster_helper, :node, :down] in names
      assert [:cluster_helper, :gen, :check] in names
    end
  end

  describe "direct emission" do
    setup do
      test_pid = self()

      :telemetry.attach_many(
        "cluster-helper-direct",
        Telemetry.event_names(),
        fn event_name, measurements, metadata, _ ->
          send(test_pid, {:direct_event, event_name, measurements, metadata})
        end,
        nil
      )

      on_exit(fn ->
        :telemetry.detach("cluster-helper-direct")
      end)

      :ok
    end

    test "emit_startup/3 sends [:cluster_helper, :startup]" do
      Telemetry.emit_startup(100, [:scope_a], [:role_a])

      assert_receive {:direct_event, [:cluster_helper, :startup], %{duration: 100},
                      %{scopes: [:scope_a], roles: [:role_a]}}
    end

    test "emit_sync_pull/2 sends [:cluster_helper, :sync, :pull]" do
      Telemetry.emit_sync_pull(:scope_b, [:"node1@host"])

      assert_receive {:direct_event, [:cluster_helper, :sync, :pull], %{count: 1},
                      %{scope: :scope_b, nodes: [:"node1@host"]}}
    end

    test "emit_gen_check/4 sends [:cluster_helper, :gen, :check]" do
      Telemetry.emit_gen_check(:"node@host", :scope_c, 50, true)

      assert_receive {:direct_event, [:cluster_helper, :gen, :check], %{duration: 50},
                      %{node: :"node@host", scope: :scope_c, changed: true}}
    end
  end
end
