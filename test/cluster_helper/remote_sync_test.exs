defmodule ClusterHelper.RemoteSync.ERPCTest do
  use ExUnit.Case, async: false

  alias ClusterHelper.RemoteSync.ERPC

  describe "safe_erpc_call/5" do
    test "succeeds for a call to a local function" do
      assert {:ok, 3} = ERPC.safe_erpc_call(node(), Kernel, :+, [1, 2], 1000)
    end

    test "captures exit signals from non-distributed node" do
      assert {:error, _} = ERPC.safe_erpc_call(:nonexistent@host, Kernel, :+, [1, 2], 100)
    end

    test "captures error from :erpc.wrap on a function that raises" do
      assert {:error, {:error, {:exception, :test_error, _}}} =
               ERPC.safe_erpc_call(node(), :erlang, :error, [:test_error], 1000)
    end

    test "returns {:ok, _} for a function returning a value" do
      assert {:ok, _node} = ERPC.safe_erpc_call(node(), :erlang, :node, [], 1000)
    end
  end

  describe "get_remote_generation/2" do
    test "returns error when node is not reachable" do
      assert {:error, _} = ERPC.get_remote_generation(:nonexistent@host, :test_scope)
    end

    test "succeeds when calling local node with default scope" do
      assert {:ok, gen} = ERPC.get_remote_generation(node(), ClusterHelper)
      assert is_integer(gen)
      assert gen >= 0
    end
  end

  describe "pull_roles_from_node/3" do
    test "returns error when node is not reachable" do
      assert {:error, _} = ERPC.pull_roles_from_node(:test_scope, :nonexistent@host)
    end

    test "succeeds when calling local node with configured scope" do
      assert {:ok, roles} = ERPC.pull_roles_from_node(ClusterHelper, node())
      assert is_list(roles)
    end
  end

  describe "get_remote_scopes/1" do
    test "returns error when node is not reachable" do
      assert {:error, _} = ERPC.get_remote_scopes(:nonexistent@host)
    end

    test "succeeds when calling local node" do
      assert {:ok, scopes} = ERPC.get_remote_scopes(node())
      assert is_list(scopes)
      assert ClusterHelper in scopes
    end
  end

  describe "noproc?/1" do
    test "returns true for {:noproc, _}" do
      assert ERPC.noproc?({:noproc, {GenServer, :call, [:registered_name, :msg]}})
    end

    test "returns true for {:exception, _, {:noproc, _}}" do
      assert ERPC.noproc?({:exception, :error, {:noproc, :details}})
    end

    test "returns false for other error reasons" do
      refute ERPC.noproc?({:nodedown, :node@host})
      refute ERPC.noproc?(:timeout)
      refute ERPC.noproc?("some_error")
    end
  end

  describe "do_pull_with_retry/4" do
    test "returns error for unreachable node with retries=3" do
      result = ERPC.do_pull_with_retry(:scope, :nonexistent@host, 5000, 3)
      assert {:error, _} = result
    end

    test "returns error for unreachable node with retries=1" do
      result = ERPC.do_pull_with_retry(:scope, :nonexistent@host, 5000, 1)
      assert {:error, _} = result
    end

    test "succeeds for local node with retries=3" do
      result = ERPC.do_pull_with_retry(ClusterHelper, node(), 5000, 3)
      assert {:ok, roles} = result
      assert is_list(roles)
    end
  end
end

defmodule ClusterHelper.RemoteSyncTest do
  use ExUnit.Case, async: false

  alias ClusterHelper.RemoteSync

  describe "behaviour delegation" do
    test "get_remote_generation delegates to configured adapter" do
      assert RemoteSync.get_remote_generation(:nonexistent@host, :scope) ==
               ClusterHelper.Adapter.for(:remote_sync).get_remote_generation(
                 :nonexistent@host,
                 :scope
               )
    end

    test "pull_roles_from_node delegates to configured adapter" do
      assert RemoteSync.pull_roles_from_node(:scope, :nonexistent@host) ==
               ClusterHelper.Adapter.for(:remote_sync).pull_roles_from_node(:scope, :nonexistent@host)
    end

    test "get_remote_scopes delegates to configured adapter" do
      assert RemoteSync.get_remote_scopes(:nonexistent@host) ==
               ClusterHelper.Adapter.for(:remote_sync).get_remote_scopes(:nonexistent@host)
    end
  end
end
