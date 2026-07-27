defmodule ClusterHelper.Config do
  @moduledoc """
  Configuration struct for `ClusterHelper`.

  Use `ClusterHelper.Config.from_app_env/0` to load configuration from
  application environment, or construct one directly for testing.
  """

  @type t :: %__MODULE__{
          scope: atom(),
          roles: [term()],
          scopes: [atom()] | nil,
          pull_interval: pos_integer(),
          pull_timeout: pos_integer(),
          event_handler: module() | nil
        }

  defstruct [:scope, :roles, :scopes, :pull_interval, :pull_timeout, :event_handler]

  @default_scope ClusterHelper
  @default_interval 7_000
  @default_timeout 5_000

  @doc """
  Builds a `Config` from application environment (`Application.get_env/2`).

  All fields have sensible defaults, so this never fails.
  """
  @spec from_app_env() :: t()
  def from_app_env do
    %__MODULE__{
      scope: Application.get_env(:cluster_helper, :scope, @default_scope),
      roles: Application.get_env(:cluster_helper, :roles, []),
      scopes: Application.get_env(:cluster_helper, :scopes, nil),
      pull_interval: Application.get_env(:cluster_helper, :pull_interval, @default_interval),
      pull_timeout: Application.get_env(:cluster_helper, :pull_timeout, @default_timeout),
      event_handler: Application.get_env(:cluster_helper, :event_handler)
    }
  end

  @doc """
  Resolves `nil` scope to the configured default scope.
  """
  @spec resolve_scope(t(), atom() | nil) :: atom()
  def resolve_scope(%__MODULE__{scope: default}, nil), do: default
  def resolve_scope(_config, scope) when is_atom(scope), do: scope
end
