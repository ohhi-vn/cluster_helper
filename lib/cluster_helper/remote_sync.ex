defmodule ClusterHelper.RemoteSync do
  @moduledoc """
  Behaviour for remote Erlang RPC operations used in cluster synchronisation.

  Implementations handle generation checks and full role pulls from remote
  nodes. The configured adapter is resolved via `ClusterHelper.Adapter`.

  ## Built-in adapters

    * `ClusterHelper.RemoteSync.ERPC` — uses `:erpc.call` (default)

  ## Custom adapters

      defmodule MyApp.MyRemoteSync do
        @behaviour ClusterHelper.RemoteSync

        @impl true
        def get_remote_generation(node, scope), do: ...
        @impl true
        def pull_roles_from_node(scope, node, opts \\ []), do: ...
        @impl true
        def get_remote_scopes(node), do: ...
      end
  """

  @doc """
  Gets the generation counter from a remote node for a given scope.

  Returns `{:ok, integer}` on success, `{:error, reason}` on failure.
  """
  @callback get_remote_generation(node(), atom()) :: {:ok, integer()} | {:error, term()}

  @doc """
  Performs a full role pull from a remote node.

  Returns `{:ok, roles}` on success, `{:error, reason}` on failure.
  """
  @callback pull_roles_from_node(atom(), node(), keyword()) :: {:ok, [term()]} | {:error, term()}

  @doc """
  Gets scopes from a remote node.

  Returns `{:ok, scopes}` on success, `{:error, reason}` on failure.
  """
  @callback get_remote_scopes(node()) :: {:ok, [atom()]} | {:error, term()}

  @doc "Gets the generation counter (delegates to configured adapter)."
  @spec get_remote_generation(node(), atom()) :: {:ok, integer()} | {:error, term()}
  def get_remote_generation(node, scope),
    do: adapter().get_remote_generation(node, scope)

  @doc "Pulls roles from a remote node (delegates to configured adapter)."
  @spec pull_roles_from_node(atom(), node(), keyword()) :: {:ok, [term()]} | {:error, term()}
  def pull_roles_from_node(scope, node, opts \\ []),
    do: adapter().pull_roles_from_node(scope, node, opts)

  @doc "Gets scopes from a remote node (delegates to configured adapter)."
  @spec get_remote_scopes(node()) :: {:ok, [atom()]} | {:error, term()}
  def get_remote_scopes(node),
    do: adapter().get_remote_scopes(node)

  defp adapter, do: ClusterHelper.Adapter.for(:remote_sync)
end
