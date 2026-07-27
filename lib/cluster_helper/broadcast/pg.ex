defmodule ClusterHelper.Broadcast.PG do
  @moduledoc """
  Broadcast adapter backed by Erlang `:pg` process groups.

  Messages are delivered to all members of a scope's `:all_nodes` group except
  the sender, using `:pg.get_members/2` + `send/2`.
  """

  @behaviour ClusterHelper.Broadcast

  alias :pg, as: Pg

  @impl true
  def start_scope(scope) do
    :pg.start(scope)
  end

  @impl true
  def join(scope) do
    Pg.join(scope, :all_nodes, self())
  end

  @impl true
  def leave(scope) do
    Pg.leave(scope, :all_nodes, self())
  end

  @impl true
  def broadcast(scope, message) do
    local_pid = self()

    scope
    |> Pg.get_members(:all_nodes)
    |> Enum.each(fn
      pid when pid != local_pid -> send(pid, message)
      _ -> :ok
    end)

    :ok
  end
end
