# Configuration

All configuration is set via `config :cluster_helper` in your application
environment.

## Reference

```elixir
config :cluster_helper,
  # Roles applied at startup in the default scope.
  # Any Elixir term is a valid role.
  roles: [:web, :api],

  # Scopes to join at startup (enables overlapping scopes).
  # When nil, defaults to a single scope named by :scope.
  scopes: [:default, :data_processing, :analytics],

  # Default scope name used when no scope is passed to API calls.
  scope: :my_cluster,

  # Timeout (ms) for remote role-pull RPC calls.
  pull_timeout: 5_000,

  # Interval (ms) between periodic background syncs.
  pull_interval: 10_000,

  # Optional event-handler module (see ClusterHelper.EventHandler).
  event_handler: MyApp.ClusterEvents
```

## Adapter Configuration

Adapters for the pluggable subsystems are configured separately:

```elixir
config :cluster_helper, ClusterHelper.Adapter,
  broadcast: ClusterHelper.Broadcast.PG,
  role_store: ClusterHelper.ETSStore,
  remote_sync: ClusterHelper.RemoteSync.ERPC
```

Each key accepts any module that implements the corresponding behaviour.
See `ClusterHelper.Adapter` and the individual behaviour docs for details.

## Defaults

| Key | Default | Description |
|-----|---------|-------------|
| `:scope` | `ClusterHelper` | Default scope atom |
| `:roles` | `[]` | Startup roles |
| `:scopes` | `nil` | Startup scopes (nil = single scope) |
| `:pull_interval` | `7_000` ms | Periodic pull interval |
| `:pull_timeout` | `5_000` ms | RPC timeout |
| `:event_handler` | `nil` | Event callback module |

## Runtime Configuration

Configuration is read at **runtime** via `Application.get_env/2`, so it can be
set in `config/runtime.exs` for environment-specific values.
