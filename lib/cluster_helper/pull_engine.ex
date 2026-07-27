defmodule ClusterHelper.PullEngine do
  @moduledoc """
  Coordinates background pull operations for cluster synchronisation.

  Manages async tasks that pull role data from remote nodes, using
  generation-based change detection to minimise unnecessary RPC traffic.
  """

  require Logger

  @task_supervisor ClusterHelper.TaskSupervisor

  @doc """
  Starts background pulls from all connected nodes for the given scope.

  Each pull runs in a separate `Task` via `Task.Supervisor`, and results
  are sent back to `server` as `{:pull_update_node, scope, node, roles}`.
  """
  @spec pull_all(pid(), atom()) :: :ok
  def pull_all(server, scope) do
    current_node = Node.self()

    live_nodes =
      Node.list()
      |> Enum.reject(&(&1 == current_node))

    Enum.each(live_nodes, &async_pull_node(server, scope, &1, :pull_update_node))
  end

  @doc """
  Generation-aware periodic pull.

  For each live node, checks the remote generation first. Only does a full
  role pull when the generation has changed. Also detects and reports stale
  nodes (in ETS but no longer in `Node.list()`).

  Results are sent back as:
    * `{:pull_update_node, scope, node, roles, gen}` — generation known
    * `{:pull_update_node, scope, node, roles}` — fallback, no generation
    * `{:stale_node, scope, node}` — node no longer in cluster
  """
  @spec pull_all_with_generation_check(pid(), atom(), %{optional(atom()) => map()}) :: :ok
  def pull_all_with_generation_check(server, scope, remote_generations) do
    current_node = Node.self()
    live_nodes = Enum.reject(Node.list(), &(&1 == current_node))
    remote_gens = Map.get(remote_generations, scope, %{})

    Task.Supervisor.start_child(@task_supervisor, fn ->
      Enum.each(live_nodes, fn node ->
        do_generation_check(server, scope, node, Map.get(remote_gens, node))
      end)

      clean_stale_nodes(server, scope, current_node, live_nodes)
    end)

    :ok
  end

  @doc """
  Pulls roles from a newly discovered node, discovering matching scopes first.
  """
  @spec pull_new_node(pid(), node(), MapSet.t(atom())) :: :ok
  def pull_new_node(server, remote_node, local_scopes) do
    Task.Supervisor.start_child(@task_supervisor, fn ->
      remote_scopes = fetch_remote_scopes(remote_node)
      matching_scopes = Enum.filter(remote_scopes, &MapSet.member?(local_scopes, &1))
      Enum.each(matching_scopes, &pull_scope_for_new_node(server, remote_node, &1))
    end)

    :ok
  end

  @doc false
  def async_pull_node(server, scope, remote_node, msg_tag) do
    Task.Supervisor.start_child(@task_supervisor, fn ->
      Logger.debug("Pulling roles from #{inspect(remote_node)} for scope #{inspect(scope)}")

      case ClusterHelper.RemoteSync.pull_roles_from_node(scope, remote_node) do
        {:ok, roles} ->
          send(server, {msg_tag, scope, remote_node, roles})

        {:error, reason} ->
          Logger.warning(
            "ClusterHelper: failed to pull from #{inspect(remote_node)}: #{inspect(reason)}"
          )
      end
    end)
  end

  @doc false
  def do_generation_check(server, scope, node, known_gen) do
    case ClusterHelper.RemoteSync.get_remote_generation(node, scope) do
      {:ok, remote_gen} when remote_gen != known_gen ->
        case ClusterHelper.RemoteSync.pull_roles_from_node(scope, node) do
          {:ok, roles} ->
            send(server, {:pull_update_node, scope, node, roles, remote_gen})

          {:error, reason} ->
            Logger.warning(
              "ClusterHelper: failed to pull from #{inspect(node)}: #{inspect(reason)}"
            )
        end

      {:ok, _same_gen} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "ClusterHelper: failed to get generation from #{inspect(node)}: #{inspect(reason)}"
        )

        case ClusterHelper.RemoteSync.pull_roles_from_node(scope, node) do
          {:ok, roles} ->
            send(server, {:pull_update_node, scope, node, roles})

          {:error, _} ->
            :ok
        end
    end
  end

  @doc false
  def clean_stale_nodes(server, scope, current_node, live_nodes) do
    known_nodes = ClusterHelper.Adapter.for(:role_store).all_nodes(scope)
    stale_nodes = Enum.reject(known_nodes, &(&1 == current_node or &1 in live_nodes))

    Enum.each(stale_nodes, fn node ->
      send(server, {:stale_node, scope, node})
    end)
  end

  @doc false
  def fetch_remote_scopes(remote_node) do
    case ClusterHelper.RemoteSync.get_remote_scopes(remote_node) do
      {:ok, scopes} when is_list(scopes) ->
        scopes

      {:ok, other} ->
        Logger.warning(
          "Unexpected scopes response from #{inspect(remote_node)}: #{inspect(other)}"
        )

        []

      {:error, {:exception, :undef, _}} ->
        Logger.debug("__get_scopes__ not found on #{inspect(remote_node)}, skipped")
        []

      {:error, reason} ->
        Logger.warning("Failed to get scopes from #{inspect(remote_node)}: #{inspect(reason)}")

        []
    end
  end

  @doc false
  def pull_scope_for_new_node(server, remote_node, scope) do
    case ClusterHelper.RemoteSync.pull_roles_from_node(scope, remote_node) do
      {:ok, roles} ->
        send_new_node_result(server, remote_node, scope, roles)

      {:error, reason} ->
        Logger.warning(
          "ClusterHelper: failed to pull from #{inspect(remote_node)}: #{inspect(reason)}"
        )
    end
  end

  @doc false
  def send_new_node_result(server, remote_node, scope, roles) do
    case ClusterHelper.RemoteSync.get_remote_generation(remote_node, scope) do
      {:ok, gen} ->
        send(server, {:pull_new_node, scope, remote_node, roles, gen})

      {:error, _} ->
        send(server, {:pull_new_node, scope, remote_node, roles})
    end
  end
end
