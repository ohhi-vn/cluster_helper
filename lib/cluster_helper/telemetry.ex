defmodule ClusterHelper.Telemetry do
  @compile {:no_warn_undefined, {:telemetry, :execute, 3}}

  @moduledoc """
  Telemetry events emitted by ClusterHelper.

  ## Events

  | Event                          | Measurements           | Metadata                                                        |
  |--------------------------------|------------------------|-----------------------------------------------------------------|
  | `[:cluster_helper, :startup]`  | `%{duration: nat}`     | `%{scopes: [atom], roles: [term]}`                              |
  | `[:cluster_helper, :role, :add]` | `%{count: pos_int}` | `%{scope: atom, roles: [term], node: node}`                     |
  | `[:cluster_helper, :role, :remove]` | `%{count: pos_int}` | `%{scope: atom, roles: [term], node: node}`                   |
  | `[:cluster_helper, :sync, :pull]` | `%{count: pos_int}` | `%{scope: atom, nodes: [node]}`                                |
  | `[:cluster_helper, :node, :up]`   | `%{}`               | `%{node: node, scopes: [atom]}`                                 |
  | `[:cluster_helper, :node, :down]` | `%{}`               | `%{node: node}`                                                 |
  | `[:cluster_helper, :gen, :check]` | `%{duration: nat}`  | `%{node: node, scope: atom, changed: boolean}`                  |

  To attach your own handler:

      :telemetry.attach_many(
        "cluster-helper-handler",
        ClusterHelper.Telemetry.event_names(),
        &MyApp.handle_cluster_telemetry/4,
        :no_config
      )

  See `:telemetry` docs for details.
  """

  @doc "Returns all event names as a list of `[prefix, ...]` lists."
  @spec event_names() :: [[atom()]]
  def event_names do
    [
      [:cluster_helper, :startup],
      [:cluster_helper, :role, :add],
      [:cluster_helper, :role, :remove],
      [:cluster_helper, :sync, :pull],
      [:cluster_helper, :node, :up],
      [:cluster_helper, :node, :down],
      [:cluster_helper, :gen, :check]
    ]
  end

  @doc false
  def emit_startup(duration, scopes, roles) do
    :telemetry.execute([:cluster_helper, :startup], %{duration: duration}, %{
      scopes: scopes,
      roles: roles
    })
  end

  @doc false
  def emit_role_add(scope, roles, node) do
    :telemetry.execute([:cluster_helper, :role, :add], %{count: length(roles)}, %{
      scope: scope,
      roles: roles,
      node: node
    })
  end

  @doc false
  def emit_role_remove(scope, roles, node) do
    :telemetry.execute([:cluster_helper, :role, :remove], %{count: length(roles)}, %{
      scope: scope,
      roles: roles,
      node: node
    })
  end

  @doc false
  def emit_sync_pull(scope, nodes) do
    :telemetry.execute([:cluster_helper, :sync, :pull], %{count: length(nodes)}, %{
      scope: scope,
      nodes: nodes
    })
  end

  @doc false
  def emit_node_up(node, scopes) do
    :telemetry.execute([:cluster_helper, :node, :up], %{}, %{node: node, scopes: scopes})
  end

  @doc false
  def emit_node_down(node) do
    :telemetry.execute([:cluster_helper, :node, :down], %{}, %{node: node})
  end

  @doc false
  def emit_gen_check(node, scope, duration, changed?) do
    :telemetry.execute([:cluster_helper, :gen, :check], %{duration: duration}, %{
      node: node,
      scope: scope,
      changed: changed?
    })
  end
end
