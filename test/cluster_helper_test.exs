defmodule ClusterHelperTest do
  use ExUnit.Case
  doctest ClusterHelper

  test "greets the world" do
    assert ClusterHelper.hello() == :world
  end
end
