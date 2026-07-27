defmodule ClusterHelper.RemoteSync.ERPC do
  @moduledoc """
  Remote sync adapter using Erlang's `:erpc.call` for distributed RPC.

  Normalises every failure mode into `{:ok, result}` / `{:error, reason}`
  and retries transient `:noproc` errors up to 3 times.
  """

  @behaviour ClusterHelper.RemoteSync

  @default_timeout 5_000
  @max_retries 3

  @impl true
  def get_remote_generation(node, scope) do
    case safe_erpc_call(node, ClusterHelper.NodeConfig, :__get_generation__, [scope], 1000) do
      {:ok, gen} when is_integer(gen) -> {:ok, gen}
      {:ok, other} -> {:error, {:bad_generation, other}}
      {:error, {:exception, :undef, _}} -> {:error, :undef}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def pull_roles_from_node(scope, node, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    do_pull_with_retry(scope, node, timeout, @max_retries)
  end

  @impl true
  def get_remote_scopes(node) do
    safe_erpc_call(node, ClusterHelper.NodeConfig, :__get_scopes__, [], 2000)
  end

  @doc """
  Wraps `:erpc.call/5`, normalising every failure mode into
  `{:ok, result}` or `{:error, reason}`.
  """
  @spec safe_erpc_call(node(), module(), atom(), [term()], non_neg_integer()) ::
          {:ok, term()} | {:error, term()}
  def safe_erpc_call(node, module, fun, args, timeout) do
    try do
      case :erpc.call(node, module, fun, args, timeout) do
        {:exception, class, reason} ->
          {:error, {:exception, class, reason}}

        result ->
          {:ok, result}
      end
    catch
      :exit, reason -> {:error, reason}
      kind, reason -> {:error, {kind, reason}}
    end
  end

  @doc false
  def do_pull_with_retry(scope, node, timeout, retries) do
    retry_timeout = if retries < 3, do: min(timeout, 1000), else: timeout

    case safe_erpc_call(node, ClusterHelper, :get_my_roles, [scope], retry_timeout) do
      {:ok, roles} when is_list(roles) ->
        {:ok, roles}

      {:error, {:exception, :undef, _}} ->
        {:error, :undef}

      {:error, reason} ->
        if noproc?(reason) and retries > 1 do
          Process.sleep(200)
          do_pull_with_retry(scope, node, timeout, retries - 1)
        else
          {:error, reason}
        end
    end
  end

  @doc false
  def noproc?({:noproc, _}), do: true
  @doc false
  def noproc?({:exception, _, {:noproc, _}}), do: true
  @doc false
  def noproc?(_), do: false
end
