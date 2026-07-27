defmodule ClusterHelper.Adapter do
  @moduledoc """
  Registry for ClusterHelper pluggable adapters.

  Each subsystem (broadcast, role store, remote sync) is defined as a behaviour
  and can be swapped at the application config level:

      config :cluster_helper, ClusterHelper.Adapter,
        broadcast: ClusterHelper.Broadcast.PG,
        role_store: ClusterHelper.ETSStore,
        remote_sync: ClusterHelper.RemoteSync.ERPC

  When no config is given each type falls back to its default adapter listed
  above, so existing projects work without any configuration change.
  """

  @type adapter_type :: :broadcast | :role_store | :remote_sync

  @doc """
  Returns the configured adapter module for the given type.

  Config can be a map or keyword list (both are common in Elixir config).
  """
  @spec for(adapter_type()) :: module()
  def for(adapter_type) do
    config = Application.get_env(:cluster_helper, __MODULE__, %{})

    case config do
      %{} -> Map.get(config, adapter_type, default_for(adapter_type))
      list when is_list(list) -> Keyword.get(list, adapter_type, default_for(adapter_type))
    end
  end

  defp default_for(:broadcast), do: ClusterHelper.Broadcast.PG
  defp default_for(:role_store), do: ClusterHelper.ETSStore
  defp default_for(:remote_sync), do: ClusterHelper.RemoteSync.ERPC
end
