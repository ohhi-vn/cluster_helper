defmodule ClusterHelper.AdapterTest do
  use ExUnit.Case, async: false

  alias ClusterHelper.Adapter

  describe "for/1" do
    test "returns default broadcast adapter when not configured" do
      assert Adapter.for(:broadcast) == ClusterHelper.Broadcast.PG
    end

    test "returns default role_store adapter when not configured" do
      assert Adapter.for(:role_store) == ClusterHelper.ETSStore
    end

    test "returns default remote_sync adapter when not configured" do
      assert Adapter.for(:remote_sync) == ClusterHelper.RemoteSync.ERPC
    end

    test "returns configured adapter from application env" do
      Application.put_env(:cluster_helper, Adapter, %{
        broadcast: ClusterHelper.Broadcast.PG,
        role_store: ClusterHelper.ETSStore,
        remote_sync: ClusterHelper.RemoteSync.ERPC
      })

      assert Adapter.for(:broadcast) == ClusterHelper.Broadcast.PG
      assert Adapter.for(:role_store) == ClusterHelper.ETSStore
      assert Adapter.for(:remote_sync) == ClusterHelper.RemoteSync.ERPC
    after
      Application.delete_env(:cluster_helper, Adapter)
    end

    test "accepts keyword list config" do
      Application.put_env(:cluster_helper, Adapter,
        broadcast: ClusterHelper.Broadcast.PG,
        role_store: ClusterHelper.ETSStore,
        remote_sync: ClusterHelper.RemoteSync.ERPC
      )

      assert Adapter.for(:broadcast) == ClusterHelper.Broadcast.PG
      assert Adapter.for(:role_store) == ClusterHelper.ETSStore
      assert Adapter.for(:remote_sync) == ClusterHelper.RemoteSync.ERPC
    after
      Application.delete_env(:cluster_helper, Adapter)
    end
  end
end
