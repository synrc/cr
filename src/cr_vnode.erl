-module(cr_vnode).
-description('Hash-ring vnode manager').
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-include_lib("kvs/include/kvs.hrl").
-include_lib("db/include/transaction.hrl").
-compile(export_all).
-record(state, {name,nodes,storage}).
-export(?GEN_SERVER).

start_link(UniqueName,HashRing) ->
    gen_server:start_link(?MODULE, [UniqueName,HashRing], []).

init([UniqueName,HashRing]) ->
    [ gen_server:cast(UniqueName,Message) || #operation{body=Message} <-
       kvs:entries(kvs:get(log,{pending,UniqueName}),operation,udnefined) ],
    error_logger:info_msg("VNODE PROTOCOL: started: ~p.~n",[UniqueName]),
    {ok,#state{name=UniqueName,nodes=HashRing}}.

handle_info({'EXIT', Pid,_}, #state{} = State) ->
    error_logger:info_msg("VNODE: EXIT~n",[]),
    {noreply, State};

handle_info(_Info, State) ->
    error_logger:info_msg("VNODE: Info ~p~n",[_Info]),
    {noreply, State}.

quorum(A) -> {ok,A}.
replay(Storage,#operation{body=Message}) -> Storage:dispatch(Message).

continuation(Next,{_,_,[],Tx}=Command,State) -> {stop, {error, servers_down}, State};
continuation(Next,{_,_,[{I,N}|T],Tx}=Command,State) ->
    Id = element(2,Tx),
    case gen_server:call({{I,N},N},Command) of
         {ok,Saved} -> {reply, kvs:add(#operation{id={sent,Id},feed_id=sent}), State};
         {error,Reason} -> continuation(Next,Command,State) end.

handle_call({pending,{_,_,[{I,N}|T],Tx}=Message}, _, #state{name=Name,storage=Storage}=State) ->
    Id = element(2,Tx),
    kvs:add(#operation{id=Id,body=Message,feed_id=Name,status=pending}),
    gen_server:cast(Name,Message),
    {reply,{ok,queued}, State};

handle_call(Request,_,Proc) ->
    error_logger:info_msg("VNODE: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast({prepare,Sender,[H|T]=Chain,Tx}=Message, #state{name=Name,storage=Storage}=State) ->
    Id = element(2,Tx),
    io:format("XA PREPARE: ~p~n",[{Tx}]),
    Val = try {ok,Op} = kvs:get(operation,Id),
              replay(Storage,Op),
              kvs:put(Op#operation{status=replayed})
       catch _:E -> {error, E} end,
    Command = case [Chain,Val] of
        [_,A={rollback,_,_,_}] -> A;
                    [[Name],_] -> {commit,H,cr:chain(Tx),Tx};
                     [[H|T],_] -> {prepare,H,T,Tx} end,
    continuation(H,Command,State);

handle_cast({commit,Sender,[H|T]=Chain,Tx}=Message, #state{name=Name,storage=Storage}=State) ->
    Id = element(2,Tx),
    io:format("XA COMMIT: ~p~n",[{Tx}]),
    Val = try {ok,Op} = kvs:get(operation,Id),
              replay(Storage,Op),
              kvs:put(Op#operation{status=commited})
       catch _:E -> {error, E} end,
    Command = case [Chain,Val] of
        [_,A={rollback,_,_,_}] -> A;
                    [[Name],_] -> stop;
                     [[H|T],_] -> {commit,H,T,Tx} end,
    continuation(H,Command,State).

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

