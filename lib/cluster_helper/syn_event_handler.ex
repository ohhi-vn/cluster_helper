
defmodule ClusterHelper.SynEventHandler do
  @moduledoc """
  `:syn` event handler for `ClusterHelper`.

  Reacts to remote scope node-up / node-down events so that the local
  `NodeConfig` stays in sync even before the periodic pull fires.

  The callback is invoked for **every** group-registration event in every syn
  scope, so each handler must guard against reasons it does not own.
  """

  @behaviour :syn_event_handler

  alias ClusterHelper.NodeConfig

  # ── :syn_event_handler callbacks ─────────────────────────────────────────────

  @impl true
  # A remote node joined our scope – pull its roles asynchronously.
  def on_process_registered(scope, _name, _pid, _meta, {:syn_remote_scope_node_up, _syn_scope, node}) do
    if scope == syn_scope() do
      NodeConfig.add_node(node)
    end

    :ok
  end

  # Ignore all other registration events (normal joins, :undefined, etc.).
  def on_process_registered(_scope, _name, _pid, _meta, _reason), do: :ok

  @impl true
  # A remote node left our scope – evict it from the local ETS table.
  def on_process_unregistered(scope, _name, _pid, _meta, {:syn_remote_scope_node_down, _syn_scope, node}) do
    if scope == syn_scope() do
      NodeConfig.remove_node(node)
    end

    :ok
  end

  # Ignore all other unregistration events.
  def on_process_unregistered(_scope, _name, _pid, _meta, _reason), do: :ok

  # ── Private ───────────────────────────────────────────────────────────────────

  defp syn_scope, do: Application.get_env(:cluster_helper, :scope, ClusterHelper)
end
