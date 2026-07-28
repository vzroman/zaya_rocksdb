-module(zaya_rocksdb_perf_SUITE).

-include_lib("common_test/include/ct.hrl").

-define(DEFAULT_PROCESSES, 100000).
-define(DEFAULT_WRITES_PER_PROCESS, 1000).
-define(DEFAULT_ENTRIES_PER_WRITE, 2).

%% CT callbacks
-export([
  all/0,
  groups/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_group/2,
  end_per_group/2,
  init_per_testcase/2,
  end_per_testcase/2
]).

%% Tests
-export([
  concurrent_write_throughput/1
]).

%%=================================================================
%%  CT CALLBACKS
%%=================================================================
all() ->
  [{group, pool_mode}, {group, direct_mode}].

groups() ->
  [{pool_mode, [], [concurrent_write_throughput]},
   {direct_mode, [], [concurrent_write_throughput]}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(zaya_rocksdb),
  Config.

end_per_suite(_Config) ->
  application:stop(zaya_rocksdb),
  ok.

init_per_group(pool_mode, Config) ->
  [{mode, pool}, {mode_params, #{}} | Config];
init_per_group(direct_mode, Config) ->
  [{mode, direct}, {mode_params, #{pool => disabled}} | Config].

end_per_group(_Group, _Config) ->
  ok.

init_per_testcase(_TestCase, Config) ->
  Mode = ?config(mode, Config),
  Dir = filename:join(?config(priv_dir, Config),
    atom_to_list(Mode) ++ "_" ++ integer_to_list(erlang:unique_integer([positive]))),
  Params = maps:merge(?config(mode_params, Config), #{dir => Dir}),
  catch zaya_rocksdb:remove(Params),
  Ref = zaya_rocksdb:create(Params),
  [{ref, Ref}, {db_dir, Dir} | Config].

end_per_testcase(_TestCase, Config) ->
  Ref = ?config(ref, Config),
  Dir = ?config(db_dir, Config),
  zaya_rocksdb:close(Ref),
  catch zaya_rocksdb:remove(#{dir => Dir}),
  ok.

%%=================================================================
%%  TESTS
%%=================================================================
concurrent_write_throughput(Config) ->
  Ref = ?config(ref, Config),
  Mode = ?config(mode, Config),

  Procs = ct:get_config(procs, ?DEFAULT_PROCESSES),
  WritesPerProc = ct:get_config(writes_per_proc, ?DEFAULT_WRITES_PER_PROCESS),
  BatchSize = ct:get_config(batch_size, ?DEFAULT_ENTRIES_PER_WRITE),

  Batches = WritesPerProc div BatchSize,
  TotalWrites = Procs * WritesPerProc,

  ct:pal("~n=== ~p mode ===~n"
         "Processes:        ~b~n"
         "Writes/process:   ~b~n"
         "Batch size:       ~b~n"
         "Batches/process:  ~b~n"
         "Total writes:     ~b~n",
         [Mode, Procs, WritesPerProc, BatchSize, Batches, TotalWrites]),

  T0 = erlang:monotonic_time(millisecond),

  Pids = [spawn_monitor(fun() ->
    worker(Ref, ProcId, Batches, BatchSize)
  end) || ProcId <- lists:seq(1, Procs)],

  Errors = collect(length(Pids)),

  Elapsed = erlang:monotonic_time(millisecond) - T0,
  ElapsedSec = Elapsed / 1000,
  Throughput = TotalWrites / ElapsedSec,

  ThroughputInt = round(Throughput),
  ct:pal("~n=== Results (~p) ===~n"
         "Elapsed:     ~p s~n"
         "Throughput:  ~p writes/s~n"
         "Errors:      ~p~n",
         [Mode, ElapsedSec, ThroughputInt, length(Errors)]),

  ct:comment("~p writes/s", [ThroughputInt]),

  case Errors of
    [] -> ok;
    _ ->
      ct:pal("First 10 errors: ~p", [lists:sublist(Errors, 10)]),
      ct:fail({errors, length(Errors)})
  end.

%%=================================================================
%%  INTERNAL
%%=================================================================
worker(Ref, ProcId, Batches, BatchSize) ->
  do_batches(Ref, ProcId, 1, Batches, BatchSize).


do_batches(_Ref, _ProcId, Seq, Batches, _BatchSize) when Seq > Batches ->
  ok;
do_batches(Ref, ProcId, Seq, Batches, BatchSize) ->
  KVs = gen_batch(ProcId, Seq, BatchSize),
  zaya_rocksdb:write(Ref, KVs),
  do_batches(Ref, ProcId, Seq + 1, Batches, BatchSize).

gen_batch(ProcId, Seq, BatchSize) ->
  Base = (ProcId bsl 32) bor (Seq * BatchSize),
  [{Base + I, {ProcId, Seq, I, erlang:monotonic_time()}} || I <- lists:seq(1, BatchSize)].


collect(Count) when Count > 0->
  receive
    {'DOWN', _Ref, process, _Pid, normal} ->
      collect(Count - 1);
    {'DOWN', _Ref, process, _Pid, Reason} ->
      [{error, {down, Reason}} | collect(Count-1)]
  end;
collect(_Count)->
  [].
