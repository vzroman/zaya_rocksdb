
-module(zaya_rocksdb).

-include("zaya_rocksdb.hrl").

-define(DEFAULT_OPEN_ATTEMPTS, 5).
-define(DESTROY_ATTEMPTS, 5).

-define(DEFAULT_ROCKSDB_OPTIONS,#{
  %compression_algorithm => todo,
  open_options=>#{
    % total_threads => 64,
    create_if_missing => false,
    %create_missing_column_families => todo,
    %error_if_exists => false,
    paranoid_checks => false,
    compression => lz4,
    compaction_style => level,
    % max_open_files => todo,
    % max_total_wal_size => todo,
    % use_fsync => todo,
    % db_paths => todo,
    % db_log_dir => todo,
    % wal_dir => todo,
    % delete_obsolete_files_period_micros => todo,
    max_background_jobs => 16,
    max_background_compactions => 8,
    % max_background_flushes => todo,
    % max_log_file_size => todo,
    % log_file_time_to_roll => todo,
    % keep_log_file_num => todo,
    % max_manifest_file_size => todo,
    % table_cache_numshardbits => todo,
    % wal_ttl_seconds => todo,
    % manual_wal_flush => todo,
    % wal_size_limit_mb => todo,
    % manifest_preallocation_size => todo,
    % allow_mmap_reads => todo,
    % allow_mmap_writes => todo,
    % is_fd_close_on_exec => todo,
    % skip_log_error_on_recovery => todo,
    % stats_dump_period_sec => todo,
    % advise_random_on_open => todo,
    % access_hint => todo,
    compaction_readahead_size => 2 * 1024 * 1024,
    % new_table_reader_for_compaction_inputs => todo,
    % use_adaptive_mutex => todo,
    % bytes_per_sync => todo,
    % skip_stats_update_on_db_open => todo,
    % wal_recovery_mode => todo,
    % allow_concurrent_memtable_write => true
    % enable_write_thread_adaptive_yield => todo,
    db_write_buffer_size => 128 * 1024 * 1024,
    max_write_buffer_number => 4,
    min_write_buffer_number_to_merge => 2,
    % recycle_log_file_num => 0,
    target_file_size_base => 256 * 1024 * 1024,
    level0_file_num_compaction_trigger => 4,
    level0_slowdown_writes_trigger => 20,
    level0_stop_writes_trigger => 36,
    max_bytes_for_level_base => 1024 * 1024 * 1024,
    max_bytes_for_level_multiplier => 8,

    block_size => 32 * 1024,

    % in_memory => todo,
    % rate_limiter => todo,
    % sst_file_manager => todo,
    % write_buffer_manager => todo,
    max_subcompactions => 8
    % atomic_flush => todo,
    % use_direct_reads => todo,
    % use_direct_io_for_flush_and_compaction => todo,
    % enable_pipelined_write => todo,
    % unordered_write => todo,
    % two_write_queues => todo,
    % statistics => todo
  },
  read => #{
    % read_tier => todo,
    verify_checksums => false,
    fill_cache => true
    % iterate_upper_bound => todo,
    % iterate_lower_bound => todo,
    % tailing => todo,
    % total_order_seek => todo,
    % prefix_same_as_start => todo,
    % snapshot => todo
  },
  write => #{
    sync => false
    % disable_wal => todo,
    % ignore_missing_column_families => todo,
    % no_slowdown => todo,
    % low_pri => todo
  }
}).

-define(DEFAULT_OPTIONS,#{
    dir => ".",
    open_attempts => ?DEFAULT_OPEN_ATTEMPTS ,
    rocksdb => ?DEFAULT_ROCKSDB_OPTIONS
}).
-define(env, maps_merge(?DEFAULT_OPTIONS, maps:from_list(application:get_all_env(zaya_rocksdb))) ).


-define(OPTIONS(O),
  maps_merge(?env, O)
).

-define(LOCK(P),P++"/LOCK").
-define(RETRY_TIMEOUT, 1000).

-define(DECODE_KEY(K), sext:decode(K) ).
-define(ENCODE_KEY(K), sext:encode(K) ).
-define(DECODE_VALUE(V), binary_to_term(V) ).
-define(ENCODE_VALUE(V), term_to_binary(V) ).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  create/1,
  open/1,
  close/1,
  remove/1
]).

%%=================================================================
%%	LOW_LEVEL API
%%=================================================================
-export([
  read/2,
  write/2,
  delete/2
]).

%%=================================================================
%%	ITERATOR API
%%=================================================================
-export([
  first/1,
  last/1,
  next/2,
  prev/2
]).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
-export([
  find/2,
  foldl/4,
  foldr/4
]).

%%=================================================================
%%	COPY API
%%=================================================================
-export([
  copy/3,
  dump_batch/2
]).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
-export([
  commit/3,
  commit1/3,
  commit2/2,
  rollback/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(ref,{ref,log,read,write,dir}).

%%=================================================================
%%	SERVICE
%%=================================================================
create( Params )->
  Options = #{
    dir := Dir,
    rocksdb := #{
      read := Read,
      write := Write
    }
  } = ?OPTIONS( maps_merge(Params, #{rocksdb => #{open_options => #{ create_if_missing => true }}}) ),

  DataDir = Dir ++"/DATA",
  LogDir = Dir++"/LOG",

  ensure_dir( DataDir ),
  ensure_dir( LogDir ),

  #ref{
    ref = try_open(DataDir, Options),
    log = try_open(LogDir, Options),
    read = maps:to_list(Read),
    write = maps:to_list(Write),
    dir = Dir
  }.

open( Params )->
  Options = #{
    dir := Dir,
    rocksdb := #{
      read := Read,
      write := Write
    }
  } = ?OPTIONS( Params ),

  DataDir = Dir ++"/DATA",
  LogDir = Dir++"/LOG",

  case filelib:is_dir( DataDir ) of
    true->
      case filelib:is_dir( LogDir ) of
        true -> ok;
        false ->
          ?LOGERROR("~s doesn't exist",[ LogDir ]),
          throw(not_exists)
      end,
      ok;
    false->
      ?LOGERROR("~s doesn't exist",[ DataDir ]),
      throw(not_exists)
  end,

  Ref = #ref{
    ref = try_open(DataDir, Options),
    log = try_open(LogDir, Options),
    read = maps:to_list(Read),
    write = maps:to_list(Write),
    dir = Dir
  },

  rollback_log( Ref ),

  Ref.


try_open(Dir, #{
  rocksdb := #{
    open_options := Params
  },
  open_attempts := Attempts
} = Options) when Attempts > 0->

  ?LOGINFO("~s try open with params ~p",[Dir, Params]),
  case rocksdb:open(Dir, maps:to_list(Params)) of
    {ok, Ref} -> Ref;
    %% Check for open errors
    {error, {db_open, Error}} ->
      % Check for hanging lock
      case lists:prefix("IO error: lock ", Error) of
        true ->
          ?LOGWARNING("~s unable to open, hanging lock, trying to unlock",[Dir]),
          case file:delete(?LOCK(Dir)) of
            ok->
              ?LOGINFO("~s lock removed, trying open",[ Dir ]),
              timer:sleep( ?RETRY_TIMEOUT ),
              % Dont decrement the attempt because we fixed the error ourselves
              try_open(Dir,Options);
            {error,UnlockError}->
              ?LOGERROR("~s lock remove error ~p, try to remove it manually",[?LOCK(Dir),UnlockError]),
              throw(locked)
          end;
        false ->
          ?LOGWARNING("~s open error ~p, try to repair left attempts ~p",[Dir,Error,Attempts-1]),
          try rocksdb:repair(Dir, [])
          catch
            _:E:S->
              ?LOGWARNING("~s repair attempt failed error ~p stack ~p, left attemps ~p",[Dir,E,S,Attempts-1])
          end,
          timer:sleep( ?RETRY_TIMEOUT ),
          try_open(Dir,Options#{ open_attempts => Attempts -1 })
      end;
    {error, Other} ->
      ?LOGERROR("~s open error ~p, left attemps ~p",[Dir,Other,Attempts-1]),
      timer:sleep( ?RETRY_TIMEOUT ),
      try_open( Dir, Options#{ open_attempts => Attempts -1 } )
  end;
try_open(Dir, #{rocksdb := Params})->
  ?LOGERROR("~s OPEN ERROR: params ~p",[Dir, Params]),
  throw(open_error).

close( #ref{ref = Ref, log = Log} )->
  case rocksdb:close( Ref ) of
    ok ->
      case rocksdb:close( Log ) of
        ok -> ok;
        {error, Error} -> throw( Error)
      end;
    {error,Error}->
      throw( Error)
  end.

remove( Params )->
  Options = #{
    dir := Dir
  } = ?OPTIONS( Params ),

  Attempts = ?DESTROY_ATTEMPTS,
  try_remove( Dir, Attempts,  Options ).

try_remove( Dir, Attempts, Options) when Attempts >0 ->
  try remove_recursive(Dir)
  catch
    _:E:S->
      ?LOGERROR("~s remove error ~p stack ~p",[Dir,E,S]),
      try_remove( Dir, Attempts -1, Options )
  end;
try_remove(Dir, 0, #{rocksdb := Params})->
  ?LOGERROR("~s REMOVE ERROR: params ~p",[Dir, Params]),
  throw(remove_error).

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(#ref{ref = Ref, read = Params}=R, [Key|Rest])->
  case rocksdb:get(Ref, ?ENCODE_KEY(Key), Params) of
    {ok, Value} ->
      [{Key,?DECODE_VALUE(Value)} | read(R,Rest) ];
    _->
      read(R, Rest)
  end;
read(_R,[])->
  [].

write(#ref{ref = Ref, write = Params}, KVs)->
  case rocksdb:write(Ref,[{put,?ENCODE_KEY(K),?ENCODE_VALUE(V)} || {K,V} <- KVs ], Params) of
    ok->ok;
    {error,Error}->throw(Error)
  end.

delete(#ref{ref = Ref, write = Params},Keys)->
  case rocksdb:write(Ref,[{delete,?ENCODE_KEY(K)} || K <- Keys], Params) of
    ok -> ok;
    {error, Error}-> throw(Error)
  end.

%%=================================================================
%%	ITERATOR
%%=================================================================
first( #ref{ref = Ref, read = Params} )->
  {ok, Itr} = rocksdb:iterator(Ref, Params),
  try
    case rocksdb:iterator_move(Itr, first) of
      {ok, K, V}->
        { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
      {error, _Error}->
        undefined
    end
  after
    catch rocksdb:iterator_close(Itr)
  end.

last( #ref{ref = Ref, read = Params} )->
  {ok, Itr} = rocksdb:iterator(Ref, Params),
  try
    case rocksdb:iterator_move(Itr, last) of
      {ok, K, V}->
        { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
      {error, _Error}->
        undefined
    end
  after
    catch rocksdb:iterator_close(Itr)
  end.

next( #ref{ref = Ref, read = Params}, K0 )->
  Key = ?ENCODE_KEY(K0),
  {ok, Itr} = rocksdb:iterator(Ref, Params),
  try
    case rocksdb:iterator_move(Itr, Key) of
      {ok, Key, _}->
        case rocksdb:iterator_move( Itr, next ) of
          {ok,K,V}->
            { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
          {error,_Error}->
            undefined
        end;
      {ok,K,V}->
        { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
      {error, _Error}->
        undefined
    end
  after
    catch rocksdb:iterator_close(Itr)
  end.

prev( #ref{ref = Ref, read = Params}, K0 )->
  Key = ?ENCODE_KEY(K0),
  {ok, Itr} = rocksdb:iterator(Ref, Params),
  try
    case rocksdb:iterator_move(Itr, Key) of
      {ok,_,_}->
        case rocksdb:iterator_move( Itr, prev ) of
          {ok,K,V}->
            { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
          {error,_Error}->
            undefined
        end;
      {error, _}->
        case rocksdb:iterator_move(Itr, last) of
          {ok,K,V}->
            { ?DECODE_KEY(K), ?DECODE_VALUE(V) };
          {error,_Error}->
            undefined
        end
    end
  after
    catch rocksdb:iterator_close(Itr)
  end.

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
%----------------------FIND------------------------------------------
find(#ref{ref = Ref, read = Params}, Query)->
  StartKey =
    case Query of
      #{start := Start}-> ?ENCODE_KEY(Start);
      _->first
    end,

  {ok, Itr} = rocksdb:iterator(Ref, [{first_key, StartKey}|Params]),
  try
    case Query of
      #{ stop:=Stop, ms:= MS, limit:=Limit }->
        StopKey = ?ENCODE_KEY(Stop),
        CompiledMS = ets:match_spec_compile(MS),
        iterate_query(rocksdb:iterator_move(Itr, StartKey), Itr, next, StopKey, CompiledMS, Limit );
      #{ stop:=Stop, ms:= MS}->
        StopKey = ?ENCODE_KEY(Stop),
        CompiledMS = ets:match_spec_compile(MS),
        iterate_ms_stop(rocksdb:iterator_move(Itr, StartKey), Itr, next, StopKey, CompiledMS );
      #{ stop:= Stop, limit := Limit }->
        iterate_stop_limit(rocksdb:iterator_move(Itr, StartKey), Itr, next, ?ENCODE_KEY(Stop), Limit );
      #{ stop:= Stop }->
        iterate_stop(rocksdb:iterator_move(Itr, StartKey), Itr, next, ?ENCODE_KEY(Stop) );
      #{ms:= MS, limit := Limit}->
        iterate_ms_limit(rocksdb:iterator_move(Itr, StartKey), Itr, next, ets:match_spec_compile(MS), Limit );
      #{ms:= MS}->
        iterate_ms(rocksdb:iterator_move(Itr, StartKey), Itr, next, ets:match_spec_compile(MS) );
      #{limit := Limit}->
        iterate_limit(rocksdb:iterator_move(Itr, StartKey), Itr, next, Limit );
      _->
        iterate(rocksdb:iterator_move(Itr, StartKey), Itr, next )
    end
  after
    catch rocksdb:iterator_close(Itr)
  end.

iterate_query({ok,K,V}, Itr, Next, StopKey, MS, Limit ) when K =< StopKey, Limit > 0->
  Rec = {?DECODE_KEY(K),?DECODE_VALUE(V) },
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_query( rocksdb:iterator_move(Itr,Next), Itr, Next, StopKey, MS, Limit - 1 )];
    []->
      iterate_query( rocksdb:iterator_move(Itr,Next), Itr, Next, StopKey, MS, Limit )
  end;
iterate_query(_, _Itr, _Next, _StopKey, _MS, _Limit )->
  [].

iterate_ms_stop({ok,K,V}, Itr, Next, StopKey, MS ) when K =< StopKey->
  Rec = {?DECODE_KEY(K),?DECODE_VALUE(V) },
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_ms_stop( rocksdb:iterator_move(Itr,Next), Itr, Next, StopKey, MS )];
    []->
      iterate_ms_stop( rocksdb:iterator_move(Itr,Next), Itr, Next, StopKey, MS )
  end;
iterate_ms_stop(_, _Itr, _Next, _StopKey, _MS )->
  [].

iterate_stop_limit({ok,K,V}, Itr, Next, StopKey, Limit ) when K =< StopKey, Limit > 0->
  [{?DECODE_KEY(K),?DECODE_VALUE(V) }| iterate_stop_limit( rocksdb:iterator_move(Itr,Next), Itr, Next, StopKey, Limit -1 )];
iterate_stop_limit(_, _Itr, _Next, _StopKey, _Limit )->
  [].

iterate_stop({ok,K,V}, Itr, Next, StopKey ) when K =< StopKey->
  [{?DECODE_KEY(K),?DECODE_VALUE(V) }| iterate_stop( rocksdb:iterator_move(Itr,Next), Itr, Next, StopKey )];
iterate_stop(_, _Itr, _Next, _StopKey )->
  [].

iterate_ms_limit({ok,K,V}, Itr, Next, MS, Limit ) when Limit >0->
  Rec = {?DECODE_KEY(K),?DECODE_VALUE(V) },
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_ms_limit( rocksdb:iterator_move(Itr,Next), Itr, Next, MS, Limit - 1 )];
    []->
      iterate_ms_limit( rocksdb:iterator_move(Itr,Next), Itr, Next, MS, Limit )
  end;
iterate_ms_limit(_, _Itr, _Next, _MS, _Limit )->
  [].

iterate_ms({ok,K,V}, Itr, Next, MS )->
  Rec = {?DECODE_KEY(K),?DECODE_VALUE(V) },
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_ms( rocksdb:iterator_move(Itr,Next), Itr, Next, MS )];
    []->
      iterate_ms( rocksdb:iterator_move(Itr,Next), Itr, Next, MS )
  end;
iterate_ms(_, _Itr, _Next, _MS )->
  [].

iterate_limit({ok,K,V}, Itr, Next, Limit) when Limit >0->
  [{?DECODE_KEY(K),?DECODE_VALUE(V) } | iterate_limit( rocksdb:iterator_move(Itr,Next), Itr, Next, Limit-1 ) ];
iterate_limit(_, _Itr, _Next, _Limit )->
  [].

iterate({ok,K,V}, Itr, Next)->
  [{?DECODE_KEY(K),?DECODE_VALUE(V) } | iterate( rocksdb:iterator_move(Itr,Next), Itr, Next ) ];
iterate(_, _Itr, _Next )->
  [].

%----------------------FOLD LEFT------------------------------------------
foldl( #ref{ref = Ref, read = Params}, Query, UserFun, InAcc )->
  StartKey =
    case Query of
      #{start := Start}-> ?ENCODE_KEY(Start);
      _->first
    end,
  Fun =
    case Query of
      #{ms:=MS}->
        CompiledMS = ets:match_spec_compile(MS),
        fun(Rec,Acc)->
          case ets:match_spec_run([Rec], CompiledMS) of
            [Res]->
              UserFun(Res,Acc);
            []->
              Acc
          end
        end;
      _->
        UserFun
    end,

  {ok, Itr} = rocksdb:iterator(Ref, [{first_key, StartKey}|Params]),
  try
    case Query of
      #{ stop:=Stop }->
        do_foldl_stop( rocksdb:iterator_move(Itr, StartKey), Itr, Fun, InAcc, ?ENCODE_KEY(Stop) );
      _->
        do_foldl( rocksdb:iterator_move(Itr, StartKey), Itr, Fun, InAcc )
    end
  catch
    {stop,Acc}->Acc
  after
    catch rocksdb:iterator_close(Itr)
  end.

do_foldl_stop( {ok,K,V}, Itr, Fun, InAcc, StopKey ) when K =< StopKey->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldl_stop( rocksdb:iterator_move(Itr,next), Itr, Fun, Acc, StopKey  );
do_foldl_stop(_, _Itr, _Fun, Acc, _StopKey )->
  Acc.

do_foldl( {ok,K,V}, Itr, Fun, InAcc )->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldl( rocksdb:iterator_move(Itr,next), Itr, Fun, Acc  );
do_foldl(_, _Itr, _Fun, Acc )->
  Acc.

%----------------------FOLD RIGHT------------------------------------------
foldr( #ref{ref = Ref, read = Params}, Query, UserFun, InAcc )->
  StartKey =
    case Query of
      #{start := Start}-> ?ENCODE_KEY(Start);
      _->last
    end,
  Fun =
    case Query of
      #{ms:=MS}->
        CompiledMS = ets:match_spec_compile(MS),
        fun(Rec,Acc)->
          case ets:match_spec_run([Rec], CompiledMS) of
            [Res]->
              UserFun(Res,Acc);
            []->
              Acc
          end
        end;
      _->
        UserFun
    end,

  {ok, Itr} = rocksdb:iterator(Ref, [{first_key, StartKey}|Params]),
  try
    case Query of
      #{ stop:=Stop }->
        do_foldr_stop( rocksdb:iterator_move(Itr, StartKey), Itr, Fun, InAcc, ?ENCODE_KEY(Stop) );
      _->
        do_foldr( rocksdb:iterator_move(Itr, StartKey), Itr, Fun, InAcc )
    end
  catch
    {stop,Acc}-> Acc
  after
    catch rocksdb:iterator_close(Itr)
  end.

do_foldr_stop( {ok,K,V}, Itr, Fun, InAcc, StopKey ) when K >= StopKey->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldr_stop( rocksdb:iterator_move(Itr,prev), Itr, Fun, Acc, StopKey  );
do_foldr_stop(_, _Itr, _Fun, Acc, _StopKey )->
  Acc.

do_foldr( {ok,K,V}, Itr, Fun, InAcc )->
  Acc = Fun( {?DECODE_KEY(K), ?DECODE_VALUE(V)}, InAcc ),
  do_foldr( rocksdb:iterator_move(Itr,prev), Itr, Fun, Acc  );
do_foldr(_, _Itr, _Fun, Acc )->
  Acc.

%%=================================================================
%%	COPY
%%=================================================================
-define(ITR_RESET_COUNT, 10000).
copy(#ref{ref = Ref, read = Params}, Fun, InAcc)->
  {ok, Itr} = rocksdb:iterator(Ref, [{first_key, first}|Params]),
  try
    do_copy( rocksdb:iterator_move(Itr, first), Itr, Fun, InAcc, ?ITR_RESET_COUNT )
  catch
    {stop,Acc}->Acc
  after
    catch rocksdb:iterator_close(Itr)
  end.
do_copy( Next, Itr, Fun, InAcc, Reset ) when Reset =:= 0->
  rocksdb:iterator_refresh(Itr),
  do_copy( Next, Itr, Fun, InAcc, ?ITR_RESET_COUNT );
do_copy( {ok,K,V}, Itr, Fun, InAcc, Reset )->
  Acc = Fun( {K, V}, InAcc ),
  do_copy( rocksdb:iterator_move(Itr,next), Itr, Fun, Acc, Reset -1 );
do_copy(_, _Itr, _Fun, Acc, _Reset )->
  Acc.

dump_batch(#ref{ref = Ref, write = Params}, KVs)->
  case rocksdb:write(Ref,[{put,K,V} || {K,V} <- KVs ], Params) of
    ok->ok;
    {error,Error}->throw(Error)
  end.

%%=================================================================
%%	TRANSACTION API
%%=================================================================
commit( #ref{ ref = DRef,write = Params}, Write, Delete )->
  Commit = prepare_commit( Write, Delete ),
  ok = rocksdb:write( DRef, Commit, Params).

commit1( #ref{ ref = DRef,log = Log ,write = Params} = Ref, Write, Delete )->
  Commit = prepare_commit( Write, Delete ),
  Rollback = prepare_rollback( Commit , Ref ),
  if
    length( Rollback ) =:=0-> ignore;
    true ->
      TRef = ?ENCODE_KEY( make_ref() ),
      try
        ok = rocksdb:write( Log, [ {put,TRef, ?ENCODE_VALUE(Rollback) }], Params),
        ok = rocksdb:write( DRef, Commit, Params),
        TRef
      catch
        _:E->
          rollback( Ref, TRef ),
          throw( E )
      end
  end.

commit2( #ref{log = Log, write = Params} , TRef )->
  if
    TRef =/= ignore ->
      ok = rocksdb:write(Log, [{delete,TRef}], Params);
    true ->
      ok
  end.

rollback(#ref{ref =Ref, log = Log ,write = Params}, TRef )->
  case rocksdb:get(Log, TRef, Params) of
    {ok, Rollback} ->
      ok = rocksdb:write( Ref, ?DECODE_VALUE( Rollback ), Params ),
      ok = rocksdb:write( Log, [{delete,TRef}], Params);
    _->
      ok
  end.

prepare_commit([{K,V}|Rest], Delete )->
  [{put,?ENCODE_KEY(K),?ENCODE_VALUE(V)} | prepare_commit(Rest, Delete) ];
prepare_commit([], [K|Rest] )->
  [{delete,?ENCODE_KEY(K)} | prepare_commit([], Rest) ];
prepare_commit([], [])->
  [].

prepare_rollback([{put,K,V}|Rest], #ref{ ref = DRef ,read = Params} = Ref)->
  case rocksdb:get(DRef, K, Params) of
    {ok, V} ->
      prepare_rollback(Rest, Ref);
    {ok, V0} ->
      [{put, K, V0} | prepare_rollback(Rest, Ref) ];
    _->
      [{delete, K} | prepare_rollback(Rest, Ref) ]
  end;

prepare_rollback([{delete, K}|Rest], #ref{ ref = DRef ,read = Params} = Ref)->
  case rocksdb:get(DRef, K, Params) of
    {ok, V} ->
      [{put, K, V} | prepare_rollback(Rest, Ref) ];
    _->
      prepare_rollback(Rest, Ref)
  end;
prepare_rollback([], _Ref)->
  [].

rollback_log( #ref{ ref = Ref, log = Log, read = ReadParams, write = WriteParams } )->
  rocksdb:fold(Log, fun({TRef, Rollback}, _Acc)->
    rocksdb:write( Ref, ?DECODE_VALUE( Rollback ), WriteParams ),
    rocksdb:write(Log, [{delete,TRef}], WriteParams)
  end, ok, ReadParams).

%%=================================================================
%%	INFO
%%=================================================================
get_size( Ref )->
  get_size( Ref, 10 ).
get_size( #ref{dir = Dir} = R, Attempts ) when Attempts > 0->
  S = list_to_binary(os:cmd("du -s --block-size=1 "++ Dir)),
  case binary:split(S,<<"\t">>) of
    [Size|_]->
      try binary_to_integer( Size )
      catch _:_->
        % Sometimes du returns error when there are some file transformations
        timer:sleep(200),
        get_size( R, Attempts - 1 )
      end;
    _ ->
      timer:sleep(200),
      get_size( R, Attempts - 1 )
  end;
get_size( _R, 0 )->
  -1.

%%=================================================================
%%	UTIL
%%=================================================================
maps_merge( Map1, Map2 )->
  maps:fold(fun(K,V2,Acc)->
    case Map1 of
      #{K := V1} when is_map(V1),is_map(V2)->
        Acc#{ K => maps_merge( V1,V2 ) };
      _->
        Acc#{ K => V2 }
    end
  end, Map1, Map2 ).


ensure_dir( Path )->
  case filelib:is_file( Path ) of
    false->
      case filelib:ensure_dir( Path++"/" ) of
        ok -> ok;
        {error,CreateError}->
          ?LOGERROR("~s create error ~p",[ Path, CreateError ]),
          throw({create_dir_error,CreateError})
      end;
    true->
      remove_recursive( Path ),
      ensure_dir( Path )
  end.

remove_recursive( Path )->
  case filelib:is_dir( Path ) of
    false->
      case filelib:is_file( Path ) of
        true->
          case file:delete( Path ) of
            ok->ok;
            {error,DelError}->
              ?LOGERROR("~s delete error ~p",[Path,DelError]),
              throw({delete_error,DelError})
          end;
        _->
          ok
      end;
    true->
      case file:list_dir_all( Path ) of
        {ok,Files}->
          [ remove_recursive(Path++"/"++F) || F <- Files ],
          case file:del_dir( Path ) of
            ok->
              ok;
            {error,DelError}->
              ?LOGERROR("~s delete error ~p",[Path,DelError]),
              throw({delete_error,DelError})
          end
      end
  end.