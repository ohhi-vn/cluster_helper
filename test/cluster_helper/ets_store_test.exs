defmodule ClusterHelper.ETSStoreTest do
  use ExUnit.Case, async: false

  alias ClusterHelper.ETSStore

  describe "has_roles?/2" do
    test "returns false when node has no roles in scope" do
      refute ETSStore.has_roles?(:nonexistent@host, :empty_scope)
    end

    test "returns true when node has roles in scope" do
      me = Node.self()
      scope = :"has_roles_test_#{System.unique_integer([:positive])}"

      ETSStore.insert_roles(me, scope, [:test_role])
      assert ETSStore.has_roles?(me, scope)

      ETSStore.delete_node(me, scope)
    end
  end
end
