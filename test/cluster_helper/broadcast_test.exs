defmodule ClusterHelper.BroadcastPGTest do
  use ExUnit.Case, async: false

  alias ClusterHelper.Broadcast.PG

  @scope ClusterHelper.Broadcast.TestScope

  setup do
    PG.start_scope(@scope)
    PG.join(@scope)

    on_exit(fn ->
      PG.leave(@scope)
    end)

    :ok
  end

  describe "join/1 and leave/1" do
    test "can leave and rejoin a scope" do
      PG.leave(@scope)

      Process.sleep(50)
      members = :pg.get_members(@scope, :all_nodes)
      refute self() in members

      assert :ok = PG.join(@scope)
      members = :pg.get_members(@scope, :all_nodes)
      assert self() in members
    end
  end

  describe "broadcast/2" do
    test "sends message to other group members but not self" do
      test_pid = self()

      listener = spawn(fn ->
        receive do
          msg -> send(test_pid, {:listener_received, msg})
        end
      end)

      :pg.join(@scope, :all_nodes, listener)

      refute_receive {:listener_received, _}

      PG.broadcast(@scope, {:broadcast_msg, "data"})

      assert_receive {:listener_received, {:broadcast_msg, "data"}}, 500
      refute_receive {:broadcast_msg, "data"}, 100
    end
  end
end
