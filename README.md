# ClusterHelper

This lib is built on top of :syn library, help to manage dynamic Elixir cluster on environment like Kubernetes.
Each node has roles like :data, :web, :cache other nodes join to cluster can easy get nodes follow a role in cluster.
Can use scope for seperating to sub cluster (current version doesn't support overlapped sub cluster).

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `cluster_helper` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:cluster_helper, "~0.0.3"}
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
  # optional, for scope of :syn lib. default scope is ClusterHelper
  scope: :my_cluster ,
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

## Test local cluster without integrated with other app

Start 2 nodes

Start node1:

```bash
iex --name node1@127.0.0.1 --cookie need_to_change_this -S mix
```

```bash
iex --name node2@127.0.0.1 --cookie need_to_change_this -S mix
```

Join & Verify all nodes by run cmd in node2 shell:

```Elixir
Node.connect(:"node1@127.0.0.1")

Node.list()
```

In each iex shell add rule by cmd:

```Elixir
ClusterHelper.add_roles([:role1, :role2])
```

Verify data synced by cmd:

```Elixir
ClusterHelper.get_nodes(:role1)
```

## Example

You can get a full example using with EasyRpc lib on [Github](https://github.com/ohhi-vn/lib_examples/tree/main/cluster_helper)
