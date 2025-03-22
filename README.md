# ClusterHelper

**TODO: Add description**

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `cluster_helper` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:cluster_helper,git: "https://github.com/ohhi-vn/cluster_helper.git", tag: "0.1.0"}
  ]
end
```

## Usage

Add rols to config for node

```Elixir
config :cluster_helper,
  # all roles of current node. Can add role in runtime.
  roles: [:data, :web],
  # for scope of :syn lib. default scope is ClusterHelper
  scope: :my_cluster 
```

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
