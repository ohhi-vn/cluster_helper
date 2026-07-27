# Getting Started

## Installation

Add `cluster_helper` to your `mix.exs` dependencies:

```elixir
def deps do
  [
    {:cluster_helper, "~> 0.7"}
  ]
end
```

## Quick Start

Nodes must be connected at the Erlang distribution level (e.g. via
[libcluster](https://hex.pm/packages/libcluster) or `Node.connect/1`).

### Tag a node with roles

```elixir
# Add roles in the default scope
ClusterHelper.add_role(:web)
ClusterHelper.add_roles([:api, "cache", {:shard, 1}])

# Add roles in a specific scope
ClusterHelper.add_role(:worker, :data_processing)
ClusterHelper.add_roles([:ingester, :aggregator], :analytics)
```

### Query the cluster

```elixir
# Find all nodes with a given role
ClusterHelper.get_nodes(:web)
#=> [:"node1@host", :"node2@host"]

# Find all roles for a node
ClusterHelper.get_roles(:"node1@host")
#=> [:web, :api]

# List every known node
ClusterHelper.all_nodes()
#=> [:"node1@host", :"node2@host"]

# Query within a specific scope
ClusterHelper.get_nodes(:worker, :data_processing)
#=> [:"node1@host"]
```

### Remove roles

```elixir
ClusterHelper.remove_role("cache")
ClusterHelper.remove_roles([{:shard, 1}])
```

### Manage scopes at runtime

```elixir
ClusterHelper.join_scope(:monitoring)
ClusterHelper.add_role(:prometheus_exporter, :monitoring)

ClusterHelper.list_scopes()
#=> [:cluster_helper, :monitoring]

ClusterHelper.leave_scope(:monitoring)
```

## Core Concepts

### Roles

A role is any Elixir term — atoms, strings, tuples, integers, or nested data:

```elixir
ClusterHelper.add_roles([:web, "cache", {:shard, 1}, 42])
```

Roles are the unit of query. You ask "which nodes have role X?" and the answer is
returned from a lock-free ETS read in microseconds.

### Scopes

A scope is an isolated namespace. Each scope maintains its own:

- Role-to-node and node-to-role mappings
- `:pg` process group for cluster messaging
- Generation counter for efficient sync

A node can have `:web` in scope `:frontend` and `:worker` in scope `:backend`
simultaneously with zero cross-talk.

### Propagation

Changes on one node propagate cluster-wide via `:pg` broadcast *and* a periodic
pull with generation-based change detection. This ensures convergence even when
broadcasts are missed.
