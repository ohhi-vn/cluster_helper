# cluster_helper/bench/cluster_helper_bench.exs
#
# Benchmark script for ClusterHelper performance testing.
#
# Run with:
#   mix run bench/cluster_helper_bench.exs
#
# Or with custom options:
#   mix run bench/cluster_helper_bench.exs --time 10 --warmup 5
#
# For before/after comparison:
#   1. Run with original code: mix run bench/cluster_helper_bench.exs > before.txt
#   2. Apply changes and run again: mix run bench/cluster_helper_bench.exs > after.txt
#   3. Compare the results

# Start the ClusterHelper application
Application.ensure_all_started(:cluster_helper)

# Get the default scope for ETS key construction
default_scope = Application.get_env(:cluster_helper, :scope, ClusterHelper)

# Clean up any existing roles
ClusterHelper.get_my_roles()
|> Enum.each(&ClusterHelper.remove_role/1)

IO.puts("""
╔══════════════════════════════════════════════════════════╗
║       ClusterHelper Performance Benchmarks               ║
╚══════════════════════════════════════════════════════════╝
""")

# ── Benchmark 1: Read Operations ──────────────────────────────────────────────

IO.puts("\n📊 Benchmark 1: Read Operations (single role lookup)\n")

# Setup: Add some roles for testing
test_roles = Enum.map(1..100, fn i -> :"role_#{i}" end)
ClusterHelper.add_roles(test_roles)

Benchee.run(
  %{
    "get_nodes/1 (existing role)" => fn ->
      Enum.each(test_roles, &ClusterHelper.get_nodes/1)
    end,
    "get_nodes/1 (non-existing role)" => fn ->
      Enum.each(1..100, fn i -> ClusterHelper.get_nodes(:"unknown_#{i}") end)
    end,
    "get_roles/1 (local node)" => fn ->
      ClusterHelper.get_roles(Node.self())
    end,
    "get_my_roles/0" => fn ->
      ClusterHelper.get_my_roles()
    end,
    "all_nodes/0" => fn ->
      ClusterHelper.all_nodes()
    end
  },
  time: 5,
  warmup: 2,
  memory_time: 2,
  formatters: [
    {Benchee.Formatters.Console, comparisons: true, extended_statistics: true}
  ]
)

# ── Benchmark 2: Write Operations ─────────────────────────────────────────────

IO.puts("\n📊 Benchmark 2: Write Operations (add/remove roles)\n")

# Clean up before write tests
ClusterHelper.get_my_roles()
|> Enum.each(&ClusterHelper.remove_role/1)

Benchee.run(
  %{
    "add_role/1 (single)" => fn ->
      role = :bench_role_single
      ClusterHelper.add_role(role)
      ClusterHelper.remove_role(role)
    end,
    "add_roles/1 (10 roles)" => fn ->
      roles = Enum.map(1..10, fn i -> :"bench_#{i}" end)
      ClusterHelper.add_roles(roles)
      ClusterHelper.remove_roles(roles)
    end,
    "add_roles/1 (100 roles)" => fn ->
      roles = Enum.map(1..100, fn i -> :"bench_#{i}" end)
      ClusterHelper.add_roles(roles)
      ClusterHelper.remove_roles(roles)
    end,
    "add_roles/1 (1000 roles)" => fn ->
      roles = Enum.map(1..1000, fn i -> :"bench_#{i}" end)
      ClusterHelper.add_roles(roles)
      ClusterHelper.remove_roles(roles)
    end,
    "remove_role/1 (single)" => fn ->
      role = :bench_remove_single
      ClusterHelper.add_role(role)
      ClusterHelper.remove_role(role)
    end,
    "remove_roles/1 (10 roles)" => fn ->
      roles = Enum.map(1..10, fn i -> :"bench_rm_#{i}" end)
      ClusterHelper.add_roles(roles)
      ClusterHelper.remove_roles(roles)
    end,
    "remove_roles/1 (100 roles)" => fn ->
      roles = Enum.map(1..100, fn i -> :"bench_rm_#{i}" end)
      ClusterHelper.add_roles(roles)
      ClusterHelper.remove_roles(roles)
    end
  },
  time: 5,
  warmup: 2,
  memory_time: 2,
  formatters: [
    {Benchee.Formatters.Console, comparisons: true, extended_statistics: true}
  ]
)

# ── Benchmark 3: Large-Scale Role Management ──────────────────────────────────

IO.puts("\n📊 Benchmark 3: Large-Scale Role Management\n")

# Clean up
ClusterHelper.get_my_roles()
|> Enum.each(&ClusterHelper.remove_role/1)

large_role_sets = %{
  "1K roles" => 1_000,
  "5K roles" => 5_000,
  "10K roles" => 10_000
}

Enum.each(large_role_sets, fn {label, count} ->
  IO.puts("\n  Testing with #{label}...")

  roles = Enum.map(1..count, fn i -> :"large_role_#{i}" end)

  {add_time, _} = :timer.tc(fn -> ClusterHelper.add_roles(roles) end)
  {get_time, _} = :timer.tc(fn -> ClusterHelper.get_my_roles() end)
  {remove_time, _} = :timer.tc(fn -> ClusterHelper.remove_roles(roles) end)

  IO.puts(
    "    Add #{count} roles:     #{:erlang.float_to_binary(add_time / 1000, decimals: 2)} ms"
  )

  IO.puts(
    "    Get #{count} roles:     #{:erlang.float_to_binary(get_time / 1000, decimals: 2)} ms"
  )

  IO.puts(
    "    Remove #{count} roles:  #{:erlang.float_to_binary(remove_time / 1000, decimals: 2)} ms"
  )
end)

# ── Benchmark 4: ETS Direct vs API ────────────────────────────────────────────

IO.puts("\n📊 Benchmark 4: ETS Direct Access vs Public API\n")

# Setup
ClusterHelper.get_my_roles()
|> Enum.each(&ClusterHelper.remove_role/1)

test_roles_ets = Enum.map(1..500, fn i -> :"ets_role_#{i}" end)
ClusterHelper.add_roles(test_roles_ets)

# ETS table names (must match node_config.ex)
ets_table = ClusterHelper.NodeConfig
ets_nodes_table = :"#{ets_table}_nodes"

IO.puts("  ETS tables: #{inspect(ets_table)} and #{inspect(ets_nodes_table)}")
IO.puts("  Default scope: #{inspect(default_scope)}\n")

Benchee.run(
  %{
    "API: get_nodes/1 (scoped)" => fn ->
      Enum.each(test_roles_ets, fn role ->
        ClusterHelper.get_nodes(role)
      end)
    end,
    "ETS: direct lookup (scoped)" => fn ->
      Enum.each(test_roles_ets, fn role ->
        :ets.lookup(ets_table, {:scope, default_scope, :role, role})
      end)
    end,
    "API: get_roles/1 (scoped)" => fn ->
      ClusterHelper.get_roles(Node.self())
    end,
    "ETS: direct lookup (node, scoped)" => fn ->
      :ets.lookup(ets_table, {:scope, default_scope, :node, Node.self()})
    end,
    "API: get_my_roles/0" => fn ->
      ClusterHelper.get_my_roles()
    end,
    "ETS: direct lookup (my roles)" => fn ->
      :ets.lookup(ets_table, {:scope, default_scope, :node, Node.self()})
    end,
    "API: all_nodes/0" => fn ->
      ClusterHelper.all_nodes()
    end,
    "ETS: select (nodes, scoped)" => fn ->
      :ets.select(ets_nodes_table, [{{:scope, default_scope, :"$1"}, [], [:"$1"]}])
    end
  },
  time: 5,
  warmup: 2,
  memory_time: 2,
  formatters: [
    {Benchee.Formatters.Console, comparisons: true, extended_statistics: true}
  ]
)

# ── Benchmark 5: Duplicate Handling ───────────────────────────────────────────

IO.puts("\n📊 Benchmark 5: Duplicate Role Handling\n")

ClusterHelper.get_my_roles()
|> Enum.each(&ClusterHelper.remove_role/1)

base_roles = Enum.map(1..100, fn i -> :"dup_role_#{i}" end)
ClusterHelper.add_roles(base_roles)

Benchee.run(
  %{
    "add_roles/1 (all duplicates)" => fn ->
      ClusterHelper.add_roles(base_roles)
    end,
    "add_roles/1 (50% new)" => fn ->
      new_roles = Enum.map(101..150, fn i -> :"dup_role_#{i}" end)
      mixed = Enum.take(base_roles, 50) ++ new_roles
      ClusterHelper.add_roles(mixed)
      # Cleanup
      ClusterHelper.remove_roles(new_roles)
    end,
    "add_roles/1 (all new)" => fn ->
      new_roles = Enum.map(200..299, fn i -> :"dup_role_new_#{i}" end)
      ClusterHelper.add_roles(new_roles)
      # Cleanup
      ClusterHelper.remove_roles(new_roles)
    end
  },
  time: 5,
  warmup: 2,
  memory_time: 2,
  formatters: [
    {Benchee.Formatters.Console, comparisons: true, extended_statistics: true}
  ]
)

# ── Benchmark 5: Concurrent Read Performance (Public vs Protected) ────────

IO.puts("\n📊 Benchmark 5: Concurrent Read Performance\n")

# Setup
ClusterHelper.get_my_roles()
|> Enum.each(&ClusterHelper.remove_role/1)

concurrent_roles = Enum.map(1..1000, fn i -> :"concurrent_#{i}" end)
ClusterHelper.add_roles(concurrent_roles)

IO.puts("  Testing with #{length(concurrent_roles)} roles")
IO.puts("  ETS table type: public (allows concurrent reads without GenServer)\n")

Benchee.run(
  %{
    "Sequential reads (API)" => fn ->
      Enum.each(1..100, fn _ ->
        ClusterHelper.get_my_roles()
      end)
    end,
    "Sequential reads (ETS direct)" => fn ->
      Enum.each(1..100, fn _ ->
        :ets.lookup(ets_table, {:scope, default_scope, :node, Node.self()})
      end)
    end,
    "Batched role lookup (API)" => fn ->
      ClusterHelper.get_nodes(:concurrent_500)
    end,
    "Batched role lookup (ETS direct)" => fn ->
      :ets.lookup(ets_table, {:scope, default_scope, :role, :concurrent_500})
    end
  },
  time: 5,
  warmup: 2,
  parallel: 4,
  formatters: [
    {Benchee.Formatters.Console, comparisons: true, extended_statistics: true}
  ]
)

# ── Cleanup ───────────────────────────────────────────────────────────────────

ClusterHelper.get_my_roles()
|> Enum.each(&ClusterHelper.remove_role/1)

IO.puts("\n✅ Benchmarks complete!")
IO.puts("\n📈 Key improvements in this version:")
IO.puts("   • ETS tables changed from :protected to :public")
IO.puts("   • get_my_roles/1 now reads directly from ETS")
IO.puts("   • Reduced code duplication in role insertion")
IO.puts("   • Better error handling in async tasks\n")
