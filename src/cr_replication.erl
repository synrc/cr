-module(cr_replication).
-description('RAFT protocol replication log backend').
-behaviour(rafter_backend).
-export([init/1, stop/1, read/2, write/2]).
-record(state, {peer :: atom() | {atom(), atom()}}).

% Issue commands only if you want them to be saved in cluster status log.

init(Peer) ->
    io:format("RAFTER BACK INIT ~p~n",[Peer]),
    State = #state{peer=Peer},
    NewState = stop(State),
    _Tid1 = ets:new(rafter, [set, named_table, public]),
    _Tid2 = ets:new(rafter_tables, [set, named_table, public]),
    NewState.

stop(State) ->
    catch ets:delete(rafter),
    catch ets:delete(rafter_tables),
    State.

read({get, Table, Key}, State) ->
    io:format("CONS GET: ~p~n",[{Table, Key}]),
    Val = try
              case ets:lookup(Table, Key) of
                  [{Key, Value}] ->
                      {ok, Value};
                  [] ->
                      {ok, not_found}
              end
          catch _:E ->
              {error, E}
          end,
     {Val, State};
read(list_tables, State) ->
    io:format("CONS DIR~n",[]),
    {{ok, [Table || {Table} <- ets:tab2list(rafter_tables)]},
        State};
read({list_keys, Table}, State) ->
    io:format("CONS ALL: ~p~n",[{Table}]),
    Val = try
              list_keys(Table)
          catch _:E ->
              {error, E}
          end,
    {Val, State};
read(_, State) ->
    {{error, ets_read_badarg}, State}.

write({new, Name}, State) ->
    io:format("CONS NEW: ~p~n",[{Name}]),
    Val = try
              _Tid = ets:new((Name), [ordered_set, named_table, public]),
              ets:insert(rafter_tables, {Name}),
              {ok, Name}
          catch _:E ->
              {error, E}
          end,
    {Val, State};

write({put, Table, Key, Value}, State) ->
    io:format("CONS PUT: ~p~n",[{Table, Key, Value}]),
    Val = try
              ets:insert(Table, {Key, Value}),
              {ok, Value}
          catch _:E ->
              {error, E}
          end,
    {Val, State};
write({delete, Table}, State) ->
    io:format("CONS DELETE: ~p~n",[{Table}]),
    Val =
        try
            ets:delete(Table),
            ets:delete(rafter_tables, Table),
            {ok, true}
        catch _:E ->
            {error, E}
        end,
    {Val, State};
write({delete, Table, Key}, State) ->
    io:format("CONS DELETE: ~p~n",[{Table,Key}]),
    Val = try
              {ok, ets:delete(Table, Key)}
          catch _:E ->
              {error, E}
          end,
    {Val, State};
write(Data, State) ->
    io:format("CONS WRITE: ~p~n",[{Data}]),
    {{error, ets_write_badarg}, State}.

list_keys(Table) ->
    list_keys(ets:first(Table), Table, []).

list_keys('$end_of_table', _Table, Keys) ->
    {ok, Keys};
list_keys(Key, Table, Keys) ->
    list_keys(ets:next(Table, Key), Table, [Key | Keys]).
