defmodule ClusterHelper.Broadcast do
  @moduledoc """
  Behaviour for cluster-wide messaging.

  Implementations handle the mechanics of delivering messages to all nodes
  in a scope. The configured adapter is resolved via `ClusterHelper.Adapter`.

  ## Built-in adapters

    * `ClusterHelper.Broadcast.PG` — Erlang `:pg` process groups (default)

  ## Custom adapters

      defmodule MyApp.MyBroadcast do
        @behaviour ClusterHelper.Broadcast

        @impl true
        def start_scope(scope), do: ...
        @impl true
        def join(scope), do: ...
        @impl true
        def leave(scope), do: ...
        @impl true
        def broadcast(scope, message), do: ...
      end

  Then configure it:

      config :cluster_helper, ClusterHelper.Adapter,
        broadcast: MyApp.MyBroadcast
  """

  @doc "Ensures the messaging scope is initialized."
  @callback start_scope(scope :: atom()) :: :ok

  @doc "Joins the current process to the scope's group."
  @callback join(scope :: atom()) :: :ok

  @doc "Leaves the current process from the scope's group."
  @callback leave(scope :: atom()) :: :ok

  @doc "Sends a message to every other member of the scope."
  @callback broadcast(scope :: atom(), message :: term()) :: :ok

  @doc "Ensures a scope is started (delegates to configured adapter)."
  @spec start_scope(atom()) :: :ok
  def start_scope(scope), do: adapter().start_scope(scope)

  @doc "Joins the current process (delegates to configured adapter)."
  @spec join(atom()) :: :ok
  def join(scope), do: adapter().join(scope)

  @doc "Leaves the current process (delegates to configured adapter)."
  @spec leave(atom()) :: :ok
  def leave(scope), do: adapter().leave(scope)

  @doc "Broadcasts a message (delegates to configured adapter)."
  @spec broadcast(atom(), term()) :: :ok
  def broadcast(scope, message), do: adapter().broadcast(scope, message)

  defp adapter, do: ClusterHelper.Adapter.for(:broadcast)
end
