# Telemetry & Monitoring

ClusterHelper emits structured events via the `:telemetry` library, allowing you
to observe cluster activity, measure latencies, and attach custom handlers.

## Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:cluster_helper, :startup]` | `%{duration: nat}` | `%{scopes: [atom], roles: [term]}` |
| `[:cluster_helper, :role, :add]` | `%{count: pos_int}` | `%{scope: atom, roles: [term], node: node}` |
| `[:cluster_helper, :role, :remove]` | `%{count: pos_int}` | `%{scope: atom, roles: [term], node: node}` |
| `[:cluster_helper, :node, :up]` | `%{}` | `%{node: node, scopes: [atom]}` |
| `[:cluster_helper, :node, :down]` | `%{}` | `%{node: node}` |

## Attaching a Handler

```elixir
:telemetry.attach_many(
  "my-app-cluster-helper",
  ClusterHelper.Telemetry.event_names(),
  &MyApp.handle_cluster_event/4,
  :no_config
)
```

### Example Handler

```elixir
defmodule MyApp.ClusterTelemetryHandler do
  def handle_cluster_event([:cluster_helper, :role, :add], %{count: count}, metadata, _config) do
    Logger.info("Added #{count} roles (#{inspect(metadata.roles)}) to #{metadata.node}")
  end

  def handle_cluster_event([:cluster_helper, :node, :up], _measurements, metadata, _config) do
    Logger.warning("Cluster node joined: #{metadata.node}")
  end

  def handle_cluster_event([:cluster_helper, :node, :down], _measurements, metadata, _config) do
    Logger.error("Cluster node left: #{metadata.node}")
  end

  def handle_cluster_event(_event, _measurements, _metadata, _config), do: :ok
end
```

## Metrics

Because ClusterHelper uses standard `:telemetry` events, you can plug in any
metric library. For example, with `Telemetry.Metrics`:

```elixir
Telemetry.Metrics.ConsoleReporter.start_link([
  {:counter, "cluster_helper.role.add.count", event: [:cluster_helper, :role, :add],
   measurement: :count, name: "cluster.roles.added"},
  {:last_value, "cluster_helper.node.up",
   event: [:cluster_helper, :node, :up], name: "cluster.node.last_join"}
])
```

## Event List

To programmatically list all event names:

```elixir
ClusterHelper.Telemetry.event_names()
#=> [[:cluster_helper, :startup], [:cluster_helper, :role, :add], ...]
```
