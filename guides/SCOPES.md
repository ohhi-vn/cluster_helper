# Overlapping Scopes

Scopes provide full isolation between logical groupings of nodes and roles.
A single node can participate in multiple scopes simultaneously, with separate
role sets, ETS tables, and generation counters for each.

## When to Use Multiple Scopes

- **Microservices** — a node acts as `:web` in the `:frontend` scope and
  `:worker` in the `:data_processing` scope
- **Multi-tenant** — each tenant gets its own scope for role isolation
- **Environment separation** — the same node serves `:staging` and
  `:production` scopes for different workloads
- **Feature flags** — gradually onboard nodes into experimental scopes

## Joining Scopes

### At startup

```elixir
config :cluster_helper,
  scopes: [:frontend, :data_processing, :analytics]
```

### At runtime

```elixir
ClusterHelper.join_scope(:monitoring)
```

## Per-Scope Operations

Every API function accepts an optional scope parameter:

```elixir
# Default scope (configured :scope or ClusterHelper)
ClusterHelper.add_role(:web)
ClusterHelper.get_nodes(:web)

# Named scope
ClusterHelper.add_role(:worker, :data_processing)
ClusterHelper.get_nodes(:worker, :data_processing)
ClusterHelper.get_roles(:"node1@host", :analytics)

# List your scopes
ClusterHelper.list_scopes()
#=> [:cluster_helper, :frontend, :data_processing, :analytics]
```

## Isolation Guarantees

Each scope has its own:

- **Role-to-node mappings** — `:web` in scope A is invisible to scope B
- **Node-to-role mappings** — same node, different roles per scope
- **`:pg` process group** — broadcasts are scoped, never leak
- **Generation counter** — sync is independent per scope
- **ETS data** — clean separation in storage

## Leaving Scopes

```elixir
ClusterHelper.leave_scope(:monitoring)
```

Leaving removes all local roles in that scope, cleans up ETS entries, and
notifies other nodes via broadcast so they can also remove stale data.
