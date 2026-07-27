defmodule ClusterHelper.ConfigTest do
  use ExUnit.Case, async: false

  alias ClusterHelper.Config

  describe "from_app_env/0" do
    setup do
      Application.delete_env(:cluster_helper, :scope)
      Application.delete_env(:cluster_helper, :roles)
      Application.delete_env(:cluster_helper, :scopes)
      Application.delete_env(:cluster_helper, :pull_interval)
      Application.delete_env(:cluster_helper, :pull_timeout)
      Application.delete_env(:cluster_helper, :event_handler)
      on_exit(fn -> cleanup_env() end)
      :ok
    end

    test "uses defaults when no config is set" do
      config = Config.from_app_env()

      assert config.scope == ClusterHelper
      assert config.roles == []
      assert config.scopes == nil
      assert config.pull_interval == 7_000
      assert config.pull_timeout == 5_000
      assert config.event_handler == nil
    end

    test "reads configured values from app env" do
      Application.put_env(:cluster_helper, :scope, :my_cluster)
      Application.put_env(:cluster_helper, :roles, [:web, :api])
      Application.put_env(:cluster_helper, :scopes, [:a, :b])
      Application.put_env(:cluster_helper, :pull_interval, 10_000)
      Application.put_env(:cluster_helper, :pull_timeout, 3_000)
      Application.put_env(:cluster_helper, :event_handler, MyApp.Handler)

      config = Config.from_app_env()

      assert config.scope == :my_cluster
      assert config.roles == [:web, :api]
      assert config.scopes == [:a, :b]
      assert config.pull_interval == 10_000
      assert config.pull_timeout == 3_000
      assert config.event_handler == MyApp.Handler
    end

    test "defaults individual fields when only some are configured" do
      Application.put_env(:cluster_helper, :scope, :custom)

      config = Config.from_app_env()

      assert config.scope == :custom
      assert config.roles == []
      assert config.pull_interval == 7_000
    end
  end

  describe "resolve_scope/2" do
    test "returns the default scope when scope is nil" do
      config = %Config{scope: :my_default}
      assert Config.resolve_scope(config, nil) == :my_default
    end

    test "returns the given scope atom when scope is not nil" do
      config = %Config{scope: :my_default}
      assert Config.resolve_scope(config, :explicit) == :explicit
    end
  end

  defp cleanup_env do
    Application.delete_env(:cluster_helper, :scope)
    Application.delete_env(:cluster_helper, :roles)
    Application.delete_env(:cluster_helper, :scopes)
    Application.delete_env(:cluster_helper, :pull_interval)
    Application.delete_env(:cluster_helper, :pull_timeout)
    Application.delete_env(:cluster_helper, :event_handler)
  end
end
