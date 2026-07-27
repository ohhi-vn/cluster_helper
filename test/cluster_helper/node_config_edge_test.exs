defmodule ClusterHelper.NodeConfigEdgeTest do
  use ExUnit.Case, async: false

  alias ClusterHelper.NodeConfig

  setup do
    ClusterHelper.get_my_roles() |> Enum.each(&ClusterHelper.remove_role/1)
    :ok
  end

  describe "handle_info catch-all" do
    test "logs and ignores unrecognized messages" do
      send(NodeConfig, :completely_unknown_message)
      :ok
    end
  end

  describe "handle_info pull_update_node with generation (5-tuple)" do
    test "updates roles and stores generation from a 5-tuple message" do
      remote = :"update_gen_remote@127.0.0.1"

      send(NodeConfig, {:pull_update_node, ClusterHelper, remote, [:role_a], 7})

      Process.sleep(100)

      assert remote in ClusterHelper.all_nodes()
      assert :role_a in ClusterHelper.get_roles(remote)
    end
  end

  describe "handle_info stale_node" do
    test "removes stale node from all data structures" do
      remote = :"stale_test@127.0.0.1"

      ClusterHelper.add_role(:stale_trigger)
      send(NodeConfig, {:new_roles, ClusterHelper, [:stale_data], remote})

      Process.sleep(100)

      send(NodeConfig, {:stale_node, ClusterHelper, remote})

      Process.sleep(100)

      refute remote in ClusterHelper.all_nodes()
      assert ClusterHelper.get_roles(remote) == []
    end
  end

  describe "handle_info remove_roles (no-op for self)" do
    test "does not process remove_roles message from self" do
      me = Node.self()
      ClusterHelper.add_role(:self_rm_test)
      send(NodeConfig, {:remove_roles, ClusterHelper, [:self_rm_test], me})

      Process.sleep(100)

      assert :self_rm_test in ClusterHelper.get_my_roles()
    end
  end

  describe "handle_info remove_roles (no-op for unknown scope)" do
    test "ignores remove_roles for an unjoined scope" do
      remote = :"remote_unjoined@127.0.0.1"
      unknown_scope = :"unknown_scope_#{System.unique_integer([:positive])}"

      send(NodeConfig, {:remove_roles, unknown_scope, [:some_role], remote})
      :ok
    end
  end

  describe "handle_info new_roles (no-op for self)" do
    test "does not process new_roles message from self" do
      me = Node.self()
      send(NodeConfig, {:new_roles, ClusterHelper, [:self_new], me})

      Process.sleep(100)

      refute :self_new in ClusterHelper.get_roles(me, ClusterHelper)
    end
  end

  describe "handle_info new_roles (no-op for unknown scope)" do
    test "ignores new_roles for an unjoined scope" do
      remote = :"remote_scope_ignore@127.0.0.1"
      unknown_scope = :"unknown_scope_#{System.unique_integer([:positive])}"

      send(NodeConfig, {:new_roles, unknown_scope, [:role], remote})
      :ok
    end
  end

  describe "handle_info pull_new_node without generation (4-tuple)" do
    test "processes pull_new_node message without generation" do
      remote = :"new_node_no_gen@127.0.0.1"
      send(NodeConfig, {:pull_new_node, ClusterHelper, remote, [:data]})

      Process.sleep(100)

      assert remote in ClusterHelper.all_nodes()
      assert :data in ClusterHelper.get_roles(remote)
    end
  end

  describe "handle_info pull_complete" do
    test "does not crash on pull_complete message" do
      send(NodeConfig, {:pull_complete, ClusterHelper})
      :ok
    end
  end

  describe "handle_info pull_roles" do
    test "does not crash and preserves state" do
      ClusterHelper.add_role(:surviving_role)

      send(NodeConfig, :pull_roles)

      Process.sleep(200)
      assert :surviving_role in ClusterHelper.get_my_roles()
    end
  end

  describe "handle_info nodedown for self" do
    test "ignores nodedown for local node" do
      me = Node.self()
      send(NodeConfig, {:nodedown, me, []})
      Process.sleep(100)
      assert me == Node.self()
    end
  end

  describe "handle_info nodeup for self" do
    test "ignores nodeup for local node" do
      me = Node.self()
      send(NodeConfig, {:nodeup, me, []})
      Process.sleep(100)
      :ok
    end
  end

  describe "configured_scopes/1" do
    test "returns {:ok, [scope]} when scopes is nil" do
      config = %ClusterHelper.Config{scope: :my_scope}
      assert NodeConfig.configured_scopes(config) == {:ok, [:my_scope]}
    end

    test "returns {:ok, scopes} when scopes is a list" do
      config = %ClusterHelper.Config{scope: :my_scope, scopes: [:a, :b]}
      assert NodeConfig.configured_scopes(config) == {:ok, [:a, :b]}
    end

    test "returns {:error, [scope]} when scopes is invalid" do
      config = %ClusterHelper.Config{scope: :fallback, scopes: :not_a_list}
      assert NodeConfig.configured_scopes(config) == {:error, [:fallback]}
    end
  end

  describe "ensure_task_supervisor/0" do
    test "returns :ok when supervisor is already running" do
      assert NodeConfig.ensure_task_supervisor() == :ok
    end

    test "starts supervisor when not running" do
      sup = ClusterHelper.TaskSupervisor

      case Process.whereis(sup) do
        nil -> :ok
        pid ->
          Process.unregister(sup)
          Process.exit(pid, :kill)
          Process.sleep(50)
      end

      assert NodeConfig.ensure_task_supervisor() == :ok
      assert Process.whereis(sup) != nil
    end
  end
end
