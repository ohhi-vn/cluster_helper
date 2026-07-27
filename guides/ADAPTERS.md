# Pluggable Adapters

ClusterHelper follows a Phoenix-like adapter pattern. Every subsystem that
interacts with the outside world is defined as an Elixir behaviour with a
built-in implementation. You can swap any adapter at the config level.

## Architecture

```
Application Config
       │
       ▼
ClusterHelper.Adapter ──► resolves to configured module
       │
       ├── ClusterHelper.Broadcast  ──► ClusterHelper.Broadcast.PG
       ├── ClusterHelper.RoleStore  ──► ClusterHelper.ETSStore
       └── ClusterHelper.RemoteSync ──► ClusterHelper.RemoteSync.ERPC
```

## Broadcast

Manages cluster-wide messaging. The built-in `Broadcast.PG` uses Erlang's `:pg`
process groups. To provide a custom broadcast layer:

```elixir
defmodule MyApp.KafkaBroadcast do
  @behaviour ClusterHelper.Broadcast

  @impl true
  def start_scope(scope), do: # ...
  @impl true
  def join(scope), do: # ...
  @impl true
  def leave(scope), do: # ...
  @impl true
  def broadcast(scope, message), do: # ...
end
```

Configure it:

```elixir
config :cluster_helper, ClusterHelper.Adapter,
  broadcast: MyApp.KafkaBroadcast
```

## RoleStore

Persists role-to-node and node-to-role mappings. The built-in `ETSStore` uses
named ETS tables with `read_concurrency: true` for microsecond lookups.

Implement the `ClusterHelper.RoleStore` behaviour to store data in Redis,
PostgreSQL, or any other backend.

## RemoteSync

Handles RPC to remote nodes for role pulls. The built-in `RemoteSync.ERPC`
uses `:erpc.call` with retry logic for transient failures.

Implement the `ClusterHelper.RemoteSync` behaviour to use a different transport.

## Testing with Mock Adapters

Swapping adapters makes testing straightforward:

```elixir
defmodule ClusterHelper.RoleStore.Mock do
  @behaviour ClusterHelper.RoleStore
  use Agent

  def start_link(_), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  @impl true
  def init, do: :ok

  @impl true
  def get_my_roles(scope), do: # ...
  @impl true
  def get_nodes(role, scope), do: # ...
  # ... etc
end
```
