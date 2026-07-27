defmodule ClusterHelper.PullEngineTest do
  use ExUnit.Case, async: false

  alias ClusterHelper.PullEngine

  defmodule MockRemoteSync do
    @behaviour ClusterHelper.RemoteSync
    use Agent

    def start_link(_opts) do
      Agent.start_link(fn -> %{} end, name: __MODULE__)
    end

    def stub_all(responses) do
      Agent.update(__MODULE__, fn _state -> responses end)
    end

    @impl true
    def get_remote_generation(_node, _scope) do
      Agent.get(__MODULE__, fn state -> Map.get(state, :get_remote_generation, {:ok, 0}) end)
    end

    @impl true
    def pull_roles_from_node(_scope, _node, _opts \\ []) do
      Agent.get(__MODULE__, fn state -> Map.get(state, :pull_roles_from_node, {:ok, []}) end)
    end

    @impl true
    def get_remote_scopes(_node) do
      Agent.get(__MODULE__, fn state -> Map.get(state, :get_remote_scopes, {:ok, []}) end)
    end
  end

  setup do
    case Agent.start_link(fn -> %{} end, name: MockRemoteSync) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    saved = Application.get_env(:cluster_helper, ClusterHelper.Adapter)
    Application.put_env(:cluster_helper, ClusterHelper.Adapter,
      Map.put(saved || %{}, :remote_sync, MockRemoteSync)
    )

    on_exit(fn ->
      try do
        Agent.stop(MockRemoteSync)
      catch
        :exit, _ -> :ok
      end

      if saved != nil do
        Application.put_env(:cluster_helper, ClusterHelper.Adapter, saved)
      else
        Application.delete_env(:cluster_helper, ClusterHelper.Adapter)
      end
    end)

    :ok
  end

  # ── fetch_remote_scopes ─────────────────────────────────────────────────────

  describe "fetch_remote_scopes/1" do
    test "returns scopes list on success" do
      MockRemoteSync.stub_all(%{get_remote_scopes: {:ok, [:a, :b]}})
      assert PullEngine.fetch_remote_scopes(:node@host) == [:a, :b]
    end

    test "returns empty list when remote returns unexpected format" do
      MockRemoteSync.stub_all(%{get_remote_scopes: {:ok, :not_a_list}})
      assert PullEngine.fetch_remote_scopes(:node@host) == []
    end

    test "returns empty list when remote has undef" do
      MockRemoteSync.stub_all(%{get_remote_scopes: {:error, {:exception, :undef, []}}})
      assert PullEngine.fetch_remote_scopes(:old@host) == []
    end

    test "returns empty list on general error" do
      MockRemoteSync.stub_all(%{get_remote_scopes: {:error, :nodedown}})
      assert PullEngine.fetch_remote_scopes(:dead@host) == []
    end
  end

  # ── pull_scope_for_new_node ─────────────────────────────────────────────────

  describe "pull_scope_for_new_node/3" do
    test "sends new_node result on successful role pull" do
      MockRemoteSync.stub_all(%{
        pull_roles_from_node: {:ok, [:role_a]},
        get_remote_generation: {:ok, 5}
      })

      PullEngine.pull_scope_for_new_node(self(), :node@host, :scope_x)

      assert_receive {:pull_new_node, :scope_x, :node@host, [:role_a], 5}, 500
    end

    test "sends new_node result without gen when gen check fails" do
      MockRemoteSync.stub_all(%{
        pull_roles_from_node: {:ok, [:role_a]},
        get_remote_generation: {:error, :timeout}
      })

      PullEngine.pull_scope_for_new_node(self(), :node@host, :scope_x)

      assert_receive {:pull_new_node, :scope_x, :node@host, [:role_a]}, 500
    end

    test "does nothing when role pull fails" do
      MockRemoteSync.stub_all(%{
        pull_roles_from_node: {:error, :nodedown}
      })

      PullEngine.pull_scope_for_new_node(self(), :dead@host, :scope_x)

      refute_receive {:pull_new_node, _, _, _, _}, 200
    end
  end

  # ── send_new_node_result ────────────────────────────────────────────────────

  describe "send_new_node_result/4" do
    test "sends with generation when available" do
      MockRemoteSync.stub_all(%{get_remote_generation: {:ok, 7}})

      PullEngine.send_new_node_result(self(), :node@host, :scope, [:r1])

      assert_receive {:pull_new_node, :scope, :node@host, [:r1], 7}, 500
    end

    test "sends without generation on error" do
      MockRemoteSync.stub_all(%{get_remote_generation: {:error, :timeout}})

      PullEngine.send_new_node_result(self(), :node@host, :scope, [:r1])

      assert_receive {:pull_new_node, :scope, :node@host, [:r1]}, 500
    end
  end

  # ── do_generation_check ─────────────────────────────────────────────────────

  describe "do_generation_check/4" do
    test "sends update with gen when gen changed and pull succeeds" do
      MockRemoteSync.stub_all(%{
        get_remote_generation: {:ok, 99},
        pull_roles_from_node: {:ok, [:new_role]}
      })

      PullEngine.do_generation_check(self(), :scope, :live@host, 0)

      assert_receive {:pull_update_node, :scope, :live@host, [:new_role], 99}, 500
    end

    test "does nothing when gen changed but pull fails" do
      MockRemoteSync.stub_all(%{
        get_remote_generation: {:ok, 99},
        pull_roles_from_node: {:error, :timeout}
      })

      PullEngine.do_generation_check(self(), :scope, :live@host, 0)

      refute_receive {:pull_update_node, _, _, _, _}, 200
    end

    test "does nothing when generation is unchanged" do
      MockRemoteSync.stub_all(%{get_remote_generation: {:ok, 5}})

      PullEngine.do_generation_check(self(), :scope, :live@host, 5)

      refute_receive {:pull_update_node, _, _, _, _}, 200
    end

    test "falls back to full pull when gen check fails and pull succeeds" do
      MockRemoteSync.stub_all(%{
        get_remote_generation: {:error, :timeout},
        pull_roles_from_node: {:ok, [:fallback]}
      })

      PullEngine.do_generation_check(self(), :scope, :live@host, 0)

      assert_receive {:pull_update_node, :scope, :live@host, [:fallback]}, 500
    end

    test "does nothing when gen check fails and fallback pull also fails" do
      MockRemoteSync.stub_all(%{
        get_remote_generation: {:error, :timeout},
        pull_roles_from_node: {:error, :nodedown}
      })

      PullEngine.do_generation_check(self(), :scope, :dead@host, 0)

      refute_receive {:pull_update_node, _, _, _, _}, 200
    end
  end

  # ── async_pull_node ─────────────────────────────────────────────────────────

  describe "async_pull_node/4" do
    test "sends update on successful role pull" do
      MockRemoteSync.stub_all(%{pull_roles_from_node: {:ok, [:r1, :r2]}})

      PullEngine.async_pull_node(self(), :scope, :node@host, :pull_update_node)

      assert_receive {:pull_update_node, :scope, :node@host, [:r1, :r2]}, 500
    end

    test "does nothing when role pull fails" do
      MockRemoteSync.stub_all(%{pull_roles_from_node: {:error, :nodedown}})

      PullEngine.async_pull_node(self(), :scope, :dead@host, :pull_update_node)

      refute_receive {:pull_update_node, _, _, _, _}, 200
    end
  end

  # ── clean_stale_nodes ───────────────────────────────────────────────────────

  describe "clean_stale_nodes/4" do
    test "sends stale_node for nodes not in live_nodes or current_node" do
      scope = :"stale_test_#{System.unique_integer([:positive])}"
      fake = :"stale@127.0.0.1"

      ClusterHelper.ETSStore.insert_roles(fake, scope, [:r])
      on_exit(fn -> ClusterHelper.ETSStore.delete_node(fake, scope) end)

      PullEngine.clean_stale_nodes(self(), scope, Node.self(), [])

      assert_receive {:stale_node, ^scope, ^fake}, 500
    end

    test "does not report current_node as stale" do
      scope = :"stale_self_#{System.unique_integer([:positive])}"
      me = Node.self()

      ClusterHelper.ETSStore.insert_roles(me, scope, [:r])
      on_exit(fn -> ClusterHelper.ETSStore.delete_node(me, scope) end)

      PullEngine.clean_stale_nodes(self(), scope, me, [])

      refute_receive {:stale_node, _, _}, 200
    end

    test "does not report nodes in live_nodes list" do
      PullEngine.clean_stale_nodes(self(), :scope, Node.self(), [:"other@host"])

      refute_receive {:stale_node, _, _}, 200
    end
  end

  # ── pull_new_node (integration via public API) ──────────────────────────────

  describe "pull_new_node/3" do
    test "discovers scopes and pulls matching roles with generation" do
      MockRemoteSync.stub_all(%{
        get_remote_scopes: {:ok, [:a, :b, :c]},
        pull_roles_from_node: {:ok, [:role]},
        get_remote_generation: {:ok, 42}
      })

      PullEngine.pull_new_node(self(), :node@host, MapSet.new([:a, :c]))

      assert_receive {:pull_new_node, :a, :node@host, [:role], 42}, 500
      assert_receive {:pull_new_node, :c, :node@host, [:role], 42}, 500
      refute_receive {:pull_new_node, :b, _, _, _}, 100
    end
  end

  # ── pull_all (no-op without live nodes) ─────────────────────────────────────

  describe "pull_all/2" do
    test "is a no-op when there are no live nodes" do
      assert PullEngine.pull_all(self(), ClusterHelper) == :ok
    end
  end
end
