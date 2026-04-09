[![Docs](https://img.shields.io/badge/api-docs-green.svg?style=flat)](https://hexdocs.pm/cluster_helper)
[![Hex.pm](https://img.shields.io/hexpm/v/cluster_helper.svg?style=flat&color=blue)](https://hex.pm/packages/cluster_helper)

# ClusterHelper

This library helps to manage dynamic Elixir clusters in environments like Kubernetes.
Each node has roles like `:data`, `:web`, `:cache` and other nodes joining the cluster can easily get nodes by role.
A configurable scope allows separation into sub-clusters with full isolation between them.

Library can use with [easy_rpc](https://hex.pm/packages/easy_rpc) for easy to develop an dynamic Elixir cluster. 

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `cluster_helper` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:cluster_helper, "~> 0.5"}
  ]
end
```

Add `:cluster_helper` to `extra_application` in mix.exs for ClusterHelper can start with your app.

```Elixir
  def application do
    [
      mod: {MyApp.Application, []},
      extra_applications: [:cluster_helper]
    ]
  end
```

## Usage

Note: You need join node to cluster by other library (like libcluster) or manually.

Add rols to config for node

```Elixir
config :cluster_helper,
  # optional, all roles of current node. Can add role in runtime.
  roles: [:data, :web],
  # optional, default scope is ClusterHelper
  scope: :my_cluster,
  # optional, default 5_000, timeout for sync between nodes.
  pull_timeout: 5_000, # ms
  # optional, default 7_000, time interval for pull from other nodes
  pull_interval: 10_000 # ms
```

If node join/leave cluster other nodes will auto update in pull event.

If add role for node other nodes will received a event for update new role of that node.

Query nodes by role in cluster

```Elixir
ClusterHelper.get_nodes(:data)

# [:node1, :node2]
```

check role of a node in cluster

```Elixir
ClusterHelper.get_roles(:node1)

# [:data, :web]
```

Support for adding callback to trigger new role or node.

```elixir
# config/config.exs
config :cluster_helper, event_handler: MyApp.ClusterEvents

# lib/my_app/cluster_events.ex
defmodule MyApp.ClusterEvents do
  @behaviour ClusterHelper.EventHandler

  @impl true
  def on_role_added(node, role) do
    Logger.info("#{node} gained role #{inspect(role)}")
  end

  @impl true
  def on_role_removed(node, role) do
    Logger.info("#{node} lost role #{inspect(role)}")
  end

  @impl true
  def on_node_added(node) do
    Phoenix.PubSub.broadcast(MyApp.PubSub, "cluster", {:node_up, node})
  end

  @impl true
  def on_node_removed(node) do
    Phoenix.PubSub.broadcast(MyApp.PubSub, "cluster", {:node_down, node})
  end
end
```

## Testing

### Running Tests

ClusterHelper has three tiers of tests. **Unit tests run by default**; cluster and scale tests require a distributed Erlang node (`--name`).

```bash
# 1. Unit tests only (fast, no distribution needed)
mix test

# 2. Cluster tests only (2-node convergence, requires --name)
mix test.cluster

# 3. Multi-node scale tests (20+ nodes, requires --name)
mix test.multi_node_scale

# 4. All tests – unit + cluster + scale
mix test.all
```

### Test Suites

| Suite | Tag | Nodes | What it tests |
|---|---|---|---|
| **Unit** | (default) | 1 (nonode) | Role CRUD, ETS consistency, event handlers, duplicate handling, any-type roles |
| **Cluster** | `:cluster` | 2-3 | Node join discovery, live role propagation, node departure cleanup, 3-node convergence |
| **Multi-Node Scale** | `:multi_node_scale` | 10-20+ | 20-node boot, role convergence, rapid churn waves, partial failure, scope isolation across nodes |

### Manual Cluster Testing

Start two nodes to test role sync manually:

```bash
# Terminal 1
iex --name node1@127.0.0.1 --cookie test_cookie -S mix

# Terminal 2
iex --name node2@127.0.0.1 --cookie test_cookie -S mix
```

In node2's shell, connect and verify:

```elixir
# Connect to node1
Node.connect(:"node1@127.0.0.1")
Node.list()
#=> [:"node1@127.0.0.1"]

# Add roles on both nodes
ClusterHelper.add_roles([:web, :api])

# Query roles across the cluster
ClusterHelper.get_nodes(:web)
#=> [:"node1@127.0.0.1", :"node2@127.0.0.1"]

ClusterHelper.get_roles(:"node1@127.0.0.1")
#=> [:web, :api]
```

### Overlapping Scopes

Test scope isolation with multiple scopes on the same nodes:

```elixir
# Join additional scopes
ClusterHelper.join_scope(:frontend)
ClusterHelper.join_scope(:backend)

# Add different roles per scope
ClusterHelper.add_role(:nginx, :frontend)
ClusterHelper.add_role(:postgres, :backend)

# Scopes are fully isolated
ClusterHelper.get_nodes(:nginx, :frontend)
#=> [:"node1@127.0.0.1", :"node2@127.0.0.1"]

ClusterHelper.get_nodes(:nginx, :backend)
#=> []

# List active scopes
ClusterHelper.list_scopes()
#=> [ClusterHelper, :frontend, :backend]
```

## Example

You can get a full example using with EasyRpc lib on [Github](https://github.com/ohhi-vn/lib_examples/tree/main/cluster_helper)

## Support AI agents & MCP for dev

Run this command for update guide & rules from deps to repo for supporting ai agents.

```bash
mix usage_rules.sync AGENTS.md --all \
  --link-to-folder deps \
  --inline usage_rules:all
```

Run this command for enable MCP server

```bash
mix tidewave
```

Config MCP for agent `http://localhost:4112/tidewave/mcp`, changes port in `mix.exs` file if needed. Go to [Tidewave](https://hexdocs.pm/tidewave/) for more informations.
