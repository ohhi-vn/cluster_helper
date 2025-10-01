defmodule ClusterHelper.SynEventHandler do
  @behaviour :syn_event_handler

  alias ClusterHelper.NodeConfig

  @impl true
  def on_process_registered(scope, _name, _pid, _meta, reason) do
    # update new node
    if scope == get_syn_scope() do
      {:syn_remote_scope_node_up, _scope, node} = reason
      NodeConfig.add_node(node)
    end
  end

  @impl true
  def on_process_unregistered(scope, _name, _pid, _meta, reason) do
    # update new node
    if scope == get_syn_scope() do
      {:syn_remote_scope_node_down, _scope, node} = reason
      NodeConfig.remove_node(node)
    end
  end

  ## Private functions

  defp get_syn_scope do
    Application.get_env(:cluster_helper, :scope, ClusterHelper)
  end
end
