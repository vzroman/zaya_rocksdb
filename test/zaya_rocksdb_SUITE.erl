-module(zaya_rocksdb_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(POOL_PARAMS, #{
  pool => #{
    size => 1,
    batch_size => 4
  }
}).

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

-export([
  default_pool_created_test/1,
  service_api_and_info_test/1,
  low_level_api_test/1,
  iterator_navigation_test/1,
  find_query_variants_test/1,
  fold_and_copy_test/1,
  dump_batch_test/1,
  transaction_api_test/1,
  prepare_rollback_roundtrip_test/1,
  is_persistent_test/1,
  concurrent_write_callers_test/1
]).

all()->
  [
    default_pool_created_test,
    {group, pool_mode},
    {group, direct_mode}
  ].

groups()->
  Tests = mode_tests(),
  [
    {pool_mode, [sequence], Tests},
    {direct_mode, [sequence], Tests}
  ].

mode_tests()->
  [
    service_api_and_info_test,
    low_level_api_test,
    iterator_navigation_test,
    find_query_variants_test,
    fold_and_copy_test,
    dump_batch_test,
    transaction_api_test,
    prepare_rollback_roundtrip_test,
    is_persistent_test,
    concurrent_write_callers_test
  ].

init_per_suite(Config)->
  {ok, _Apps} = application:ensure_all_started(zaya_rocksdb),
  Config.

end_per_suite(_Config)->
  ok = application:stop(zaya_rocksdb),
  ok.

init_per_group(pool_mode, Config)->
  [{mode, pool}, {mode_params, ?POOL_PARAMS} | Config];
init_per_group(direct_mode, Config)->
  [{mode, direct}, {mode_params, #{pool => disabled}} | Config];
init_per_group(_Group, Config)->
  Config.

end_per_group(_Group, _Config)->
  ok.

init_per_testcase(TestCase, Config)->
  Dir =
    filename:join(
      ?config(priv_dir, Config),
      atom_to_list(TestCase) ++ "_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
  [{db_dir, Dir} | Config].

end_per_testcase(_TestCase, Config)->
  Dir = ?config(db_dir, Config),
  catch zaya_rocksdb:remove(#{dir => Dir}),
  ok.

%%=================================================================
%%  TESTS
%%=================================================================
default_pool_created_test(Config)->
  Params = #{dir => ?config(db_dir, Config)},
  catch zaya_rocksdb:remove(Params),
  Ref = zaya_rocksdb:create(Params),
  try
    ?assert(is_pid(ref_pool(Ref))),
    ok = zaya_rocksdb:write(Ref, [{default_key, default_val}]),
    ?assertEqual([{default_key, default_val}], zaya_rocksdb:read(Ref, [default_key]))
  after
    ok = zaya_rocksdb:close(Ref)
  end.

service_api_and_info_test(Config)->
  Params = backend_params(Config),
  catch zaya_rocksdb:remove(Params),

  Ref1 = zaya_rocksdb:create(Params),
  try
    assert_mode_ref(Config, Ref1),
    ?assertEqual(undefined, zaya_rocksdb:first(Ref1)),
    ?assertEqual(undefined, zaya_rocksdb:last(Ref1)),
    ok = zaya_rocksdb:write(Ref1, [{service_key, service_value}]),
    ?assert(zaya_rocksdb:get_size(Ref1) > 0)
  after
    ok = zaya_rocksdb:close(Ref1)
  end,

  Ref2 = zaya_rocksdb:open(Params),
  try
    assert_mode_ref(Config, Ref2),
    ?assertEqual(
      [{service_key, service_value}],
      zaya_rocksdb:read(Ref2, [service_key])
    )
  after
    ok = zaya_rocksdb:close(Ref2)
  end,

  ?assertEqual(ok, zaya_rocksdb:remove(Params)).

low_level_api_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_rocksdb:write(Ref, sample_records()),
      ?assertEqual(
        [{5, five}, {1, one}, {3, three}],
        zaya_rocksdb:read(Ref, [5, 1, 99, 3])
      ),

      ok = zaya_rocksdb:write(Ref, []),
      ok = zaya_rocksdb:delete(Ref, [3, 42]),
      ?assertEqual(
        #{1 => one, 5 => five, 7 => seven},
        read_map(Ref, [1, 5, 7])
      ),

      ok = zaya_rocksdb:delete(Ref, []),
      ?assertEqual([], zaya_rocksdb:read(Ref, [3, 42]))
    end
  ).

iterator_navigation_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ?assertEqual(undefined, zaya_rocksdb:first(Ref)),
      ?assertEqual(undefined, zaya_rocksdb:last(Ref)),
      ?assertEqual(undefined, zaya_rocksdb:next(Ref, 1)),
      ?assertEqual(undefined, zaya_rocksdb:prev(Ref, 1)),

      ok = zaya_rocksdb:write(Ref, sample_records()),
      ?assertEqual({1, one}, zaya_rocksdb:first(Ref)),
      ?assertEqual({7, seven}, zaya_rocksdb:last(Ref)),
      ?assertEqual({3, three}, zaya_rocksdb:next(Ref, 1)),
      ?assertEqual({3, three}, zaya_rocksdb:next(Ref, 2)),
      ?assertEqual(undefined, zaya_rocksdb:next(Ref, 7)),
      ?assertEqual({5, five}, zaya_rocksdb:prev(Ref, 7)),
      ?assertEqual({5, five}, zaya_rocksdb:prev(Ref, 6)),
      ?assertEqual(undefined, zaya_rocksdb:prev(Ref, 1))
    end
  ).

find_query_variants_test(Config)->
  MSAtLeastThree = [{{'$1', '$2'}, [{'>=', '$1', 3}], ['$_']}],
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_rocksdb:write(Ref, sample_records()),

      ?assertEqual(sample_records(), zaya_rocksdb:find(Ref, #{})),
      ?assertEqual(
        [{3, three}, {5, five}, {7, seven}],
        zaya_rocksdb:find(Ref, #{start => 2})
      ),
      ?assertEqual(
        [{1, one}, {3, three}, {5, five}],
        zaya_rocksdb:find(Ref, #{stop => 5})
      ),
      ?assertEqual(
        [{3, three}, {5, five}],
        zaya_rocksdb:find(Ref, #{start => 2, stop => 5})
      ),
      ?assertEqual(
        [{1, one}, {3, three}],
        zaya_rocksdb:find(Ref, #{limit => 2})
      ),
      ?assertEqual(
        [{3, three}, {5, five}],
        zaya_rocksdb:find(Ref, #{start => 2, stop => 5, limit => 2})
      ),
      ?assertEqual(
        [{3, three}, {5, five}, {7, seven}],
        zaya_rocksdb:find(Ref, #{ms => MSAtLeastThree})
      ),
      ?assertEqual(
        [{3, three}, {5, five}],
        zaya_rocksdb:find(Ref, #{ms => MSAtLeastThree, limit => 2})
      ),
      ?assertEqual(
        [{3, three}, {5, five}],
        zaya_rocksdb:find(Ref, #{start => 2, stop => 5, ms => MSAtLeastThree})
      ),
      ?assertEqual(
        [{3, three}],
        zaya_rocksdb:find(Ref, #{start => 2, stop => 7, ms => MSAtLeastThree, limit => 1})
      )
    end
  ).

fold_and_copy_test(Config)->
  MSValuesAtLeastThree = [{{'$1', '$2'}, [{'>=', '$1', 3}], ['$2']}],
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_rocksdb:write(Ref, sample_records()),

      %% foldl with ms
      ?assertEqual(
        [seven, five, three],
        zaya_rocksdb:foldl(
          Ref,
          #{ms => MSValuesAtLeastThree},
          fun(Value, Acc)-> [Value | Acc] end,
          []
        )
      ),

      %% foldr with start (start => 5 starts at key 5 and goes backward)
      ?assertEqual(
        [1, 3, 5],
        zaya_rocksdb:foldr(
          Ref,
          #{start => 5},
          fun({Key, _Value}, Acc)-> [Key | Acc] end,
          []
        )
      ),

      %% foldl with stop
      ?assertEqual(
        [five, three, one],
        zaya_rocksdb:foldl(
          Ref,
          #{stop => 5},
          fun({_K, V}, Acc)-> [V | Acc] end,
          []
        )
      ),

      %% foldr with stop
      ?assertEqual(
        [three, five, seven],
        zaya_rocksdb:foldr(
          Ref,
          #{stop => 3},
          fun({_K, V}, Acc)-> [V | Acc] end,
          []
        )
      ),

      %% foldl early termination
      ?assertEqual(
        [{5, five}, {3, three}, {1, one}],
        zaya_rocksdb:foldl(
          Ref,
          #{},
          fun({5, five} = Rec, Acc)-> throw({stop, [Rec | Acc]});
             (Rec, Acc)-> [Rec | Acc]
          end,
          []
        )
      ),

      %% copy (receives raw encoded {K, V} pairs)
      CopyResult = zaya_rocksdb:copy(
        Ref,
        fun({K, V}, Acc)-> [{sext:decode(K), binary_to_term(V)} | Acc] end,
        []
      ),
      ?assertEqual(
        [{7, seven}, {5, five}, {3, three}, {1, one}],
        CopyResult
      )
    end
  ).

dump_batch_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      RawBatch = [
        {sext:encode(raw_a), term_to_binary(10)},
        {sext:encode(raw_b), term_to_binary(20)}
      ],
      ok = zaya_rocksdb:dump_batch(Ref, RawBatch),
      ?assertEqual(
        #{raw_a => 10, raw_b => 20},
        read_map(Ref, [raw_a, raw_b])
      )
    end
  ).

transaction_api_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_rocksdb:write(Ref, [{keep, 1}, {drop, 2}, {stay, 3}]),

      %% commit
      ok = zaya_rocksdb:commit(Ref, [{keep, 10}, {add, 11}], [drop]),
      ?assertEqual(
        #{keep => 10, add => 11, stay => 3},
        read_map(Ref, [keep, add, stay])
      ),
      ?assertEqual([], zaya_rocksdb:read(Ref, [drop])),

      %% empty commit
      ok = zaya_rocksdb:commit(Ref, [], [])
    end
  ).

prepare_rollback_roundtrip_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_rocksdb:write(Ref, [{item, original}]),
      {RollbackWrite, RollbackDelete} =
        zaya_rocksdb:prepare_rollback(Ref, [{item, updated}], []),
      ok = zaya_rocksdb:commit(Ref, [{item, updated}], []),
      ok = zaya_rocksdb:commit(Ref, RollbackWrite, RollbackDelete),
      ?assertEqual([{item, original}], zaya_rocksdb:read(Ref, [item]))
    end
  ).

is_persistent_test(_Config)->
  ?assertEqual(true, zaya_rocksdb:is_persistent()).

concurrent_write_callers_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      Parent = self(),
      Keys = lists:seq(1, 8),
      [
        spawn(fun()->
          Parent ! {writer_result, Key, catch zaya_rocksdb:write(Ref, [{Key, Key * 10}])}
        end)
       || Key <- Keys
      ],
      Results = collect_results(length(Keys), []),
      ?assertEqual([], [Result || {_Key, Result} <- Results, Result =/= ok]),
      ?assertEqual(
        maps:from_list([{Key, Key * 10} || Key <- Keys]),
        read_map(Ref, Keys)
      )
    end
  ).

%%=================================================================
%%  HELPERS
%%=================================================================
with_ref(Config, Fun)->
  Params = backend_params(Config),
  catch zaya_rocksdb:remove(Params),
  Ref = zaya_rocksdb:create(Params),
  try
    assert_mode_ref(Config, Ref),
    Fun(Ref)
  after
    ok = zaya_rocksdb:close(Ref)
  end.

backend_params(Config)->
  ModeParams = ?config(mode_params, Config),
  Base = #{dir => ?config(db_dir, Config)},
  case ModeParams of
    undefined -> Base;
    _ -> maps:merge(ModeParams, Base)
  end.

assert_mode_ref(Config, Ref)->
  case ?config(mode, Config) of
    pool ->
      ?assert(is_pid(ref_pool(Ref)));
    direct ->
      ?assertEqual(disabled, ref_pool(Ref));
    _ ->
      ok
  end.

ref_pool(Ref)->
  element(tuple_size(Ref), Ref).

sample_records()->
  [{1, one}, {3, three}, {5, five}, {7, seven}].

read_map(Ref, Keys)->
  maps:from_list(zaya_rocksdb:read(Ref, Keys)).

collect_results(0, Results)->
  Results;
collect_results(Count, Results)->
  receive
    {writer_result, Key, Result}->
      collect_results(Count - 1, [{Key, Result} | Results])
  after
    5000 ->
      ct:fail(timeout)
  end.
