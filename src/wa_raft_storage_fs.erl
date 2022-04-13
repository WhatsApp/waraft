%%% Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.
%%%
%%% This source code is licensed under the Apache 2.0 license found in
%%% the LICENSE file in the root directory of this source tree.
%%%
%%% This module implements storage interface by using Linux btrfs.
%%%
%%% The reasons that we choose btrfs over other Linux filesystems are:
%%%   1) Cool BTree implementation which support billions files easily
%%%   2) Point-in-time COW snapshot so that we could create a storage
%%%      snapshot instantaneously for RAFT InstallSnapshot API
%%%   3) File checksum which can detect byte flip or silent disk failure

-module(wa_raft_storage_fs).
-compile(warn_missing_spec).
-author('huiliu@fb.com').

-behavior(wa_raft_storage).

%% Mandatory callbacks
-export([
    storage_open/1,
    storage_close/2,
    storage_apply/3,
    storage_create_snapshot/2,
    storage_create_empty_snapshot/2,
    storage_delete_snapshot/2,
    storage_open_snapshot/2,
    storage_status/1
]).

%% Callbacks to storage I/O
-export([
    storage_write/5,
    storage_read/2,
    storage_read_version/2,
    storage_delete/3,
    storage_write_metadata/4,
    storage_read_metadata/2
]).

-export([
    update_last_log/3,
    fullpath/3
]).

-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/logger.hrl").
-include("wa_raft.hrl").

-type storage_handle() :: {wa_raft:table(), wa_raft:partition()}.

%% Data directory levels
-define(MAX_DIR_LEVELS, 16).
%% Max file size - 10MB
-define(MAX_FILE_SIZE, 10485760).
%% Tag in key for metadata
-define(METADATA_TAG, '$metadata').
%% Last applied log id
-define(LAST_LOG_ID, "__LAST__").
%% Raft version cache LRU
-define(RAFT_VERSION_CACHE_LRU(Table, Partition), ?TO_ATOM("raft_version_cache_lru_", Table, Partition)).
%% Raft version cache
-define(RAFT_VERSION_CACHE(Table, Partition), ?TO_ATOM("raft_version_cache_", Table, Partition)).

%%%-------------------------------------------------------------------
%%%
%%% Implementation of storage callbacks
%%%
%%%-------------------------------------------------------------------

%% Storage callback to open
-spec storage_open(#raft_storage{}) -> {wa_raft_log:log_pos(), storage_handle()}.
storage_open(#raft_storage{table = Table, partition = Partition, root_dir = RootDir}) ->
    filelib:ensure_dir(RootDir),
    init_version_cache(Table, Partition),
    DbDir = ?DATA_DIR(Table, Partition),
    LastApplied =
        case filelib:is_dir(DbDir) of
            true ->
                LastLog = last_log(Table, Partition),
                ?LOG_NOTICE("Last applied log in data directory ~s is ~200p. btrfs ~p", [DbDir, LastLog, ?RAFT_CONFIG(raft_enable_btrfs, true)], #{domain => [whatsapp, wa_raft]}),
                LastLog;
            _ ->
                ?LOG_NOTICE("Data directory ~s is missing. Create empty one. btrfs ~p", [DbDir, ?RAFT_CONFIG(raft_enable_btrfs, true)], #{domain => [whatsapp, wa_raft]}),
                fs_create(DbDir),
                #raft_log_pos{}
        end,
    {LastApplied, {Table, Partition}}.

%% Storage callback to close
-spec storage_close(storage_handle(), #raft_storage{}) -> ok.
storage_close({Table, Partition}, #raft_storage{last_applied = LastApplied}) ->
    update_last_log(Table, Partition, LastApplied),
    ok.

%% Storage callback to create snapshot
-spec storage_create_snapshot(string(), #raft_storage{}) -> ok | wa_raft_storage:error().
storage_create_snapshot(Name, #raft_storage{table = Table, partition = Partition, root_dir = RootDir}) ->
    SnapDir = RootDir ++ Name,
    DbDir = ?DATA_DIR(Table, Partition),
    case filelib:is_dir(DbDir) of
        true ->
            % take snapshot if db dir exists
            fs_snapshot(DbDir, SnapDir);
        false ->
            % create empty snapshot if db dir doesn't exist
            fs_create(SnapDir)
    end.

%% Storage callback to create empty snapshot
-spec storage_create_empty_snapshot(string(), #raft_storage{}) -> ok | wa_raft_storage:error().
storage_create_empty_snapshot(Name, #raft_storage{root_dir = RootDir}) ->
    SnapDir = RootDir ++ Name,
    case filelib:is_dir(SnapDir) of
        true ->
            fs_remove(SnapDir);
        false ->
            ok
    end,
    % create empty snapshot
    fs_create(SnapDir).

%% Storage callback to delete snapshot
-spec storage_delete_snapshot(string(), #raft_storage{}) -> ok | wa_raft_storage:error().
storage_delete_snapshot(Name, #raft_storage{root_dir = RootDir}) ->
    SnapDir = RootDir ++ Name,
    case filelib:is_dir(SnapDir) of
        true ->
            fs_remove(SnapDir);
        false ->
            ok
    end.

%% Storage callback to open snapshot
-spec storage_open_snapshot(#raft_snapshot{}, #raft_storage{}) -> {wa_raft_log:log_pos(), storage_handle()} | wa_raft_storage:error().
storage_open_snapshot(#raft_snapshot{name = Name, last_applied = LogPos},
                      #raft_storage{table = Table, partition = Partition, root_dir = RootDir}) ->
    SnapDir = RootDir ++ Name,
    DbDir = ?DATA_DIR(Table, Partition),
    fs_remove(DbDir),
    case exec("/bin/mv " ++ SnapDir ++ " " ++ DbDir) of
        ok ->
            init_version_cache(Table, Partition),
            ensure_data_dir(DbDir),
            {LogPos, {Table, Partition}};
        Error ->
            Error
    end.

-spec storage_status(Handle :: storage_handle()) -> wa_raft_storage:status().
storage_status(_Handle) ->
    [].

%% Storage callback to apply an op
-spec storage_apply(Command :: wa_raft_acceptor:command(), LogPos :: wa_raft_log:log_pos(), DbHandle :: storage_handle()) -> {ok, storage_handle()} | {{ok, Return :: term()}, storage_handle()} | {wa_raft_storage:error(), storage_handle()}.
storage_apply({read, _Table, Key}, _LogPos, DbHandle) ->
    Result = case storage_read(DbHandle, Key) of
        {ok, {LogIndex, _LogTerm, Header, Value}} -> {ok, {Value, Header, LogIndex}};
        {error, _} = Error -> Error
    end,
    {Result, DbHandle};
storage_apply({delete, _Table, Key}, LogPos, DbHandle) ->
    {storage_delete(DbHandle, LogPos, Key), DbHandle};
storage_apply({write, _Table, Key, Value}, LogPos, DbHandle) ->
    {storage_write(DbHandle, LogPos, Key, Value, #{}), DbHandle};
storage_apply(noop, _LogPos, DbHandle) ->
    {ok, DbHandle};
storage_apply(Command, LogPos, DbHandle) ->
    ?LOG_ERROR("Unrecognized command ~0p at ~0p", [Command, LogPos], #{domain => [whatsapp, wa_raft]}),
    {{error, enotsup}, DbHandle}.

%% Write key value pair to storage.
%%
%% How to support concurrent read/write from different processes: 2 phase write
%%  - Write to shadow file. Read still goes to data file.
%%  - After write is done, rename the shadow file to correct data file.
%% UNIX rename syscall is atomic. Read process either read data file or shadow file
-spec storage_write(storage_handle(), wa_raft_log:log_pos(), term(), binary(), map()) -> {ok, wa_raft_log:log_index()} | wa_raft_storage:error().
storage_write({Table, Partition}, #raft_log_pos{index = LogIndex, term = LogTerm}, Key, Value, Header) ->
    Fullpath = fullpath(Table, Partition, Key),
    FullpathShadow = Fullpath ++ ".shadow",
    HeaderLength = 0,
    Data = <<LogIndex:64/unsigned, LogTerm:64/unsigned, HeaderLength:64/unsigned, Value/binary>>,
    case byte_size(Data) > ?MAX_FILE_SIZE of
        true ->
            ?RAFT_COUNT('raft.fs.error.toobig'),
            ?LOG_ERROR("write too big (~s) ~p > ~p", [Fullpath, byte_size(Data), ?MAX_FILE_SIZE], #{domain => [whatsapp, wa_raft]});
        false ->
            ok
    end,
    ?LOG_DEBUG("[~p, ~p:~p] write file for key ~p", [Partition, LogIndex, LogTerm, Key], #{domain => [whatsapp, wa_raft]}),
    case file_write(FullpathShadow, Data) of %% phase 1 - write to shadow file
        ok ->
            case prim_file:rename(FullpathShadow, Fullpath) of %% phase 2 rename
                ok ->
                    update_version_cache(Table, Partition, Key, LogIndex),
                    maps:get(readonly, Header, false) andalso file_set_readonly(Fullpath),
                    {ok, LogIndex};
                RenameError ->
                    ?RAFT_COUNT('raft.fs.error.rename'),
                    ?LOG_ERROR("file rename error (~s) ~p", [FullpathShadow, RenameError], #{domain => [whatsapp, wa_raft]}),
                    RenameError
            end;
        WriteError ->
            WriteError
    end.

%% Read value for given key
-spec storage_read(storage_handle(), term()) ->
    {ok, {wa_raft_log:log_index() | undefined, wa_raft_log:log_term() | undefined, map(), binary() | undefined}} | wa_raft_storage:error().
storage_read({Table, Partition}, Key) ->
    Fullpath = fullpath(Table, Partition, Key),
    file_read(Fullpath).

%% Read headers for given key
-spec storage_read_version(storage_handle(), term()) -> wa_raft_log:log_index() | undefined | wa_raft_storage:error().
storage_read_version({Table, Partition}, Key) ->
    case lookup_version_cache(Table, Partition, Key) of
        [] ->
            Fullpath = fullpath(Table, Partition, Key),
            Version = file_read_version(Fullpath),
            Version;
        [{Key, Version, _Ts}] ->
            ?RAFT_COUNT('raft.fs.read_version.cache.hit'),
            Version
    end.

%% Delete key from storage
-spec storage_delete(storage_handle(), wa_raft_log:log_pos(), term()) -> ok | wa_raft_storage:error().
storage_delete({Table, Partition}, #raft_log_pos{index = LogIndex, term = LogTerm}, Key) ->
    Fullpath = fullpath(Table, Partition, Key),
    ?LOG_DEBUG("[~p, ~p:~p] delete file for key ~p", [Partition, LogIndex, LogTerm, Key], #{domain => [whatsapp, wa_raft]}),
    case file_delete(Fullpath) of
        ok ->
            update_version_cache(Table, Partition, Key, undefined);
        Other ->
            Other
    end.

%% Write key value pair for given metadata key
-spec storage_write_metadata(storage_handle(), wa_raft_storage:metadata(), wa_raft_log:log_pos(), term()) -> ok | wa_raft_storage:error().
storage_write_metadata(Handle, Key, Version, Value) ->
    case storage_write(Handle, Version, {?METADATA_TAG, Key}, term_to_binary(Value), #{}) of
        {ok, _Index} -> ok;
        Other        -> Other
    end.

%% Read value for given metadata key
-spec storage_read_metadata(storage_handle(), wa_raft_storage:metadata()) -> {ok, wa_raft_log:log_pos(), term()} | undefined | wa_raft_storage:error().
storage_read_metadata(Handle, Key) ->
    case storage_read(Handle, {?METADATA_TAG, Key}) of
        {ok, {undefined, _, _, _}} ->
            undefined;
        {ok, {Index, Term, _Meta, Binary}} ->
            {ok, #raft_log_pos{index = Index, term = Term}, binary_to_term(Binary)};
        {error, enoent} ->
            undefined;
        Other ->
            Other
    end.

%%%-------------------------------------------------------------------
%%%
%%%  Private functions
%%%
%%%-------------------------------------------------------------------

%% Full path for given key
-spec fullpath(wa_raft:table(), wa_raft:partition(), term()) -> string().
fullpath(Table, Partition, Key) ->
    Filename = filename(Key),
    lists:concat([?DATA_DIR(Table, Partition), directory(Key), "/", Filename]).

%% Generate file name for key. A key could be
%%  integer e.g 1
%%  string e.g "123"
%%  binary e.g <<"123">>
%%  tuple e.g {1, 2, 3}, or {1, "2", {3, 4}}
-spec filename(term()) -> term().
filename(Key) when is_tuple(Key) ->
    List0 = flatten(Key, []),
    List1 = lists:flatmap(fun(E) -> [filename(E)] end, List0),
    lists:concat(lists:join("-", List1));
filename(Key) when is_integer(Key) ->
    integer_to_list(Key);
filename(Key) when is_binary(Key) ->
    binary_to_list(Key);
filename(Key) ->
    Key.

%% Convert a tuple to a flat list recursively
-spec flatten(list() | tuple(), list()) -> list().
flatten([], Acc) ->
    lists:flatten(lists:reverse(Acc));
flatten(Tuple, Acc) when is_tuple(Tuple) ->
    flatten(tuple_to_list(Tuple), Acc);
flatten([Tuple | Tail], Acc) when is_tuple(Tuple) ->
    flatten(tuple_to_list(Tuple) ++ Tail, Acc);
flatten([Head | Tail], Acc) ->
    flatten(Tail, [Head | Acc]).

%% Delete a file
-spec file_delete(string()) -> ok | wa_raft_storage:error().
file_delete(Filename) ->
    case prim_file:delete(Filename) of
        ok ->
            ok;
        {error, enoent} ->
            ?RAFT_COUNT('raft.fs.delete.non.existent.file'),
            ok;
        {error, _} = Error ->
            ?RAFT_COUNT('raft.fs.error.delete'),
            ?LOG_ERROR("delete error (~s) ~p", [Filename, Error]),
            Error
    end.

%% Write data to file
-spec file_write(string(), binary()) -> ok | wa_raft_storage:error().
file_write(Filename, Data) ->
    ?RAFT_GATHER('raft.fs.write.size', byte_size(Data)),
    case prim_file:write_file(Filename, Data) of
        ok ->
            ok;
        Error ->
            ?RAFT_COUNT('raft.fs.error.write'),
            ?LOG_ERROR("write file (~s) error ~0p", [Filename, Error], #{domain => [whatsapp, wa_raft]}),
            Error
    end.

%% Read file content
-spec file_read(string()) -> {ok, {wa_raft_log:log_index() | undefined, wa_raft_log:log_term() | undefined, map(), binary() | undefined}} | wa_raft_storage:error().
file_read(Filename) ->
    case prim_file:open(Filename, [read, binary]) of
        {ok, Fd} ->
            try prim_file:read(Fd, ?MAX_FILE_SIZE * 10) of % read up to 100 MB file
                {ok, <<LogIndex:64/unsigned, LogTerm:64/unsigned, HeaderLength:64/unsigned, HeaderAndValue/binary>>} ->
                    case byte_size(HeaderAndValue) > HeaderLength of
                        true ->
                            <<_HeaderBin:HeaderLength/binary, Value/binary>> = HeaderAndValue,
                            {ok, {LogIndex, LogTerm, #{}, Value}};
                        false ->
                            ?RAFT_COUNT('raft.fs.read.error'),
                            ?LOG_ERROR("corrupted file (~s): expect at least ~p bytes, but got ~p", [Filename, HeaderLength, HeaderAndValue], #{domain => [whatsapp, wa_raft]}),
                            {ok, {undefined, undefined, #{}, undefined}}
                    end;
                {ok, <<>>} ->
                    ?RAFT_COUNT('raft.fs.read.empty'),
                    ?LOG_ERROR("read empty file (~s)", [Filename], #{domain => [whatsapp, wa_raft]}),
                    {ok, {undefined, undefined, #{}, undefined}};
                eof ->
                    ?RAFT_COUNT('raft.fs.read.eof'),
                    ?LOG_ERROR("read eof (~s)", [Filename], #{domain => [whatsapp, wa_raft]}),
                    {ok, {undefined, undefined, #{}, undefined}};
                Result ->
                    ?RAFT_COUNT('raft.fs.error.read'),
                    ?LOG_ERROR("read error (~s) ~p", [Filename, Result], #{domain => [whatsapp, wa_raft]}),
                    Result
            after
                file:close(Fd)
            end;
        {error, enoent} ->
            {ok, {undefined, undefined, #{}, undefined}};
        Error ->
            ?RAFT_COUNT('raft.fs.error.open'),
            ?LOG_ERROR("open error (~s) ~p", [Filename, Error], #{domain => [whatsapp, wa_raft]}),
            Error
    end.

%% Read file version
-spec file_read_version(string()) -> wa_raft_log:log_index() | undefined | wa_raft_storage:error().
file_read_version(Filename) ->
    case prim_file:open(Filename, [raw, {read_ahead, 16}]) of
        {ok, Fd} ->
            try prim_file:read(Fd, 16) of
                {ok, <<LogIndex:64/unsigned, _LogTerm:64/unsigned>>} ->
                    LogIndex;
                {ok, CorruptedHeader} ->
                    {error, {corrupted, CorruptedHeader}};
                eof ->
                    {error, eof};
                Error ->
                    ?RAFT_COUNT('raft.fs.error.read_version'),
                    ?LOG_ERROR("read version error (~s) ~p", [Filename, Error], #{domain => [whatsapp, wa_raft]}),
                    Error
            after
                file:close(Fd)
            end;
        {error, enoent} ->
            undefined;
        Error ->
            ?RAFT_COUNT('raft.fs.error.open'),
            ?LOG_ERROR("open error (~s) ~p", [Filename, Error], #{domain => [whatsapp, wa_raft]}),
            Error
    end.

-spec file_set_readonly(string()) -> ok | wa_raft_storage:error().
file_set_readonly(FullPath) ->
    case file:change_mode(FullPath, 8#444) of
        ok ->
            ok;
        Error ->
            ?RAFT_COUNT('raft.fs.error.change_mode'),
            ?LOG_ERROR("change mode error (~s) ~p", [FullPath, Error], #{domain => [whatsapp, wa_raft]}),
            Error
    end.

%% Generic filesystem operations
-spec fs_snapshot(string(), string()) -> ok | wa_raft_storage:error().
fs_snapshot(SrcDir, DestDir) ->
    case ?RAFT_CONFIG(raft_enable_btrfs, true) of
        true ->
            btrfs_snapshot(SrcDir, DestDir);
        false ->
            exec(lists:concat(["/bin/cp -R --reflink=auto ", SrcDir, " ", DestDir]))
    end.

-spec fs_remove(string()) -> ok | wa_raft_storage:error().
fs_remove(Dir) ->
    case ?RAFT_CONFIG(raft_enable_btrfs, true) of
        true ->
            btrfs_remove(Dir);
        false ->
            exec(lists:concat(["/bin/rm -fR ", Dir]))
    end.

-spec fs_create(string()) -> ok.
fs_create(DbDir) ->
    case ?RAFT_CONFIG(raft_enable_btrfs, true) of
        true ->
            btrfs_create(DbDir);
        false ->
            filelib:ensure_dir(DbDir)
    end,
    ensure_data_dir(DbDir),
    ok.

%% BTRFS
-spec btrfs_snapshot(string(), string()) -> ok | wa_raft_storage:error().
btrfs_snapshot(SrcDir, DestDir) ->
    exec("/usr/sbin/btrfs subvolume snapshot -r " ++ SrcDir ++ " " ++ DestDir).

-spec btrfs_remove(string()) -> ok | wa_raft_storage:error().
btrfs_remove(Dir) ->
    % TODO(shobhit): Remove command to update permissions when we can delete ro files using a non-root user
    case btrfs_make_writable(Dir) of
        ok ->
            exec(lists:concat(["btrfs subvolume delete ", Dir]));
        E ->
            E
    end.

-spec btrfs_make_writable(string()) -> ok | wa_raft_storage:error().
btrfs_make_writable(Dir) ->
    exec(lists:concat(["btrfs property set ", Dir, " ", "ro false"])).

-spec btrfs_create(string()) -> ok | wa_raft_storage:error().
btrfs_create(Dir) ->
    exec("/usr/sbin/btrfs subvolume create " ++ Dir).

%% Execute os command. os:cmd/1 doesn't return exit code. Need use erlang:open_port to get both stdout and exit code
-spec exec(Command :: string()) -> ok | wa_raft_storage:error().
exec(Command) ->
    Port = erlang:open_port({spawn, Command}, [stream, in, eof, hide, exit_status, stderr_to_stdout]),
    ?LOG_NOTICE("Starting '~s'. ~p", [Command, Port], #{domain => [whatsapp, wa_raft]}),
    case wait_for_exec(Port, []) of
        {0, Output} ->
            ?LOG_NOTICE("Command '~s' is done: ~s", [Command, Output], #{domain => [whatsapp, wa_raft]}),
            ok;
        {ExitCode, Output} ->
            ?LOG_NOTICE("Command '~s' error ~p: ~s", [Command, ExitCode, Output], #{domain => [whatsapp, wa_raft]}),
            {error, efault}
    end.

%% Wait for completion of port and return both exit code and stdout
-spec wait_for_exec(Port :: port(), Output :: string()) -> {ExitCode :: integer(), Output :: string()}.
wait_for_exec(Port, Output) ->
    receive
        {Port, {data, Bytes}} ->
            wait_for_exec(Port, [Output | Bytes]);
        {Port, eof} ->
            ExitCode = check_exit_code(Port),
            {ExitCode, lists:flatten(Output)}
    end.

%% Check exit code for a completed command
-spec check_exit_code(Port :: port()) -> ExitCode :: integer().
check_exit_code(Port) ->
    % close port
    Port ! {self(), close},
    receive
        {Port, closed} ->
            true
    end,
    % wait for exit
    receive
        {'EXIT',  Port,  _} ->
            ok
    after 1 ->  % force context switch
        ok
    end,
    % check exit code
    receive
        {Port, {exit_status, Code}} ->
            Code
    end.

-spec update_last_log(wa_raft:table(), wa_raft:partition(), wa_raft_log:log_pos()) -> ok | wa_raft_storage:error().
update_last_log(Table, Partition, LogPos) ->
    DbDir = ?DATA_DIR(Table, Partition),
    case filelib:is_dir(DbDir) of
        true ->
            ?LOG_NOTICE("Write last log ~p for ~p:~p", [LogPos, Table, Partition], #{domain => [whatsapp, wa_raft]}),
            Path = filename:join([DbDir, ?LAST_LOG_ID]),
            Data = term_to_binary(LogPos),
            file_write(Path, Data);
        false ->
            ok
    end.

-spec last_log(wa_raft:table(), wa_raft:partition()) -> wa_raft_log:log_pos().
last_log(Table, Partition) ->
    Path = filename:join([?DATA_DIR(Table, Partition), ?LAST_LOG_ID]),
    case prim_file:read_file(Path) of
        {ok, Data} when byte_size(Data) > 0 ->
            prim_file:delete(Path), %% Delete last log after read. It'll be written again when stopping
            binary_to_term(Data);
        _ ->
            #raft_log_pos{}
    end.

-spec init_version_cache(wa_raft:table(), wa_raft:partition()) -> ok.
init_version_cache(Table, Partition) ->
    VersionCache = ?RAFT_VERSION_CACHE(Table, Partition),
    ets:whereis(VersionCache) =/= undefined andalso ets:delete(VersionCache),
    ets:new(VersionCache, [named_table, set, public]),

    VersionCacheLru = ?RAFT_VERSION_CACHE_LRU(Table, Partition),
    ets:whereis(VersionCacheLru) =/= undefined andalso ets:delete(VersionCacheLru),
    ets:new(VersionCacheLru, [named_table, ordered_set, public]),
    ets:insert(VersionCacheLru, {size, 0}),
    ok.

-spec lookup_version_cache(wa_raft:table(), wa_raft:partition(), term()) -> list().
lookup_version_cache(Table, Partition, Key) ->
    try
        ets:lookup(?RAFT_VERSION_CACHE(Table, Partition), Key)
    catch
        _T:Error ->
            ?RAFT_COUNT('raft.fs.error.lookup_cache'),
            ?LOG_ERROR("Version cache lookup error ~p ~p for key ~p. error ~p", [Table, Partition, Key, Error], #{domain => [whatsapp, wa_raft]}),
            []
    end.

-spec update_version_cache(wa_raft:table(), wa_raft:partition(), term(), term()) -> ok.
update_version_cache(Table, Partition, Key, Result) ->
    try
        Tab = ?RAFT_VERSION_CACHE(Table, Partition),
        LRU = ?RAFT_VERSION_CACHE_LRU(Table, Partition),
        Size = add_version_cache(Tab, LRU, Key, Result),
        maybe_cleanup_version_cache(Tab, LRU, Size)
    catch
        _T:Error:Stack ->
            ?RAFT_COUNT('raft.fs.error.update_cache'),
            ?LOG_ERROR("Version cache update error ~p ~p for key ~p. error ~p~n~0p", [Table, Partition, Key, Error, Stack], #{domain => [whatsapp, wa_raft]}),
            ok
    end.

-spec add_version_cache(ets:tab(), ets:tab(), term(), term()) -> non_neg_integer().
add_version_cache(Tab, LRU, Key, Result) ->
    % Delete if it has been cached before
    maybe_delete_version_cache_lru(Tab, LRU, Key),

    NowTs = erlang:system_time(microsecond),
    ets:insert(Tab, {Key, Result, NowTs}),
    ets:insert(LRU, {NowTs, Key}),
    ets:update_counter(LRU, size, 1).

-spec maybe_delete_version_cache_lru(ets:tab(), ets:tab(), term()) -> ok.
maybe_delete_version_cache_lru(Tab, LRU, Key) ->
    case ets:lookup(Tab, Key) of
        [{_Key, _Result0, Ts}] ->
            ets:update_counter(LRU, size, -1),
            ets:delete(LRU, Ts);
        [] ->
            ok
    end.

-spec maybe_cleanup_version_cache(ets:tab(), ets:tab(), non_neg_integer()) -> ok.
maybe_cleanup_version_cache(Tab, LRU, Size) ->
    case Size > ?RAFT_CONFIG(raft_version_cache_size, 1000000) of
        true ->
            FirstTs = ets:first(LRU),
            [{FirstTs, ToBeDeletedKey}] = ets:lookup(LRU, FirstTs),
            ets:delete(Tab, ToBeDeletedKey),
            ets:delete(LRU, FirstTs),
            ets:update_counter(LRU, size, -1),
            ok;
        false ->
            ok
    end.

-spec max_levels() -> pos_integer().
max_levels() -> ?MAX_DIR_LEVELS * ?MAX_DIR_LEVELS.

-spec directory(term()) -> iolist().
directory(Key) ->
    DirKey =
        case is_tuple(Key) of
            true -> element(1, Key); %% Pick first element in tuple as dir key
            false -> Key
        end,
    Hash = erlang:phash2(DirKey, max_levels()),
    lists:concat([directory_fmt(Hash div ?MAX_DIR_LEVELS), "/", directory_fmt(Hash rem ?MAX_DIR_LEVELS)]).

%% Use it to replace slow io_lib:format("~2.16.0b", V)
directory_fmt(V) ->
    List = "0" ++ integer_to_list(V, 16),
    string:to_lower(lists:sublist(List, length(List) - 1, 2)).

-spec ensure_data_dir(string()) -> ok.
ensure_data_dir(DbDir) ->
    Levels = ?MAX_DIR_LEVELS,
    Dirs = [[DbDir, directory_fmt(X), directory_fmt(Y), "."] || X <- lists:seq(0, Levels - 1), Y <- lists:seq(0, Levels - 1)],
    [filelib:ensure_dir(filename:join(Dir)) || Dir <- Dirs],
    ?LOG_NOTICE("Ensured db dir ~s. created ~p subdirs", [DbDir, length(Dirs)], #{domain => [whatsapp, wa_raft]}).
