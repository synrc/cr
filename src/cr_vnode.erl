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

continuation(Next,{_,_,[],Tx}=Command,State) -> {noreply, State};
continuation(Next,{C,S,[{I,N}|T],Tx}=Command,State) ->
    Id = element(2,Tx),
    Peer = cr:peer({I,N}),
    io:format("{I:N:P}=~p~n",[{I,N,Peer}]),
    Vpid = cr:vpid(I,Peer),
    io:format("{I:N:P:V}=~p~n",[{I,N,Peer,Vpid}]),
    io:format("continuation call(~p,~p)~n",[{Vpid,Peer},{pending,Command}]),
    case gen_server:call(Vpid,{pending,Command}) of
         {ok,Saved} -> io:format("CAST OK~n"), {noreply,State};
         {error,Reason} -> io:format("CAST ERROR~n"),
            timer:sleep(1000), continuation(Next,Command,State) end.

handle_call({pending,{Cmd,Self,[{I,N}|T],Tx}=Message}, _, #state{name=Name,storage=Storage}=State) ->
    Id = element(2,Tx),
    io:format("XA QUEUE: ~p~n",[{Id,Message,Name}]),
    Res = kvs:add(#operation{id=Id,body=Message,feed_id=Name,status=pending}),
    This = self(), spawn(fun() -> gen_server:cast(This,Message) end),
    {reply,{ok,queued}, State};

handle_call(Request,_,Proc) ->
    error_logger:info_msg("VNODE: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast({prepare,Sender,[H|T]=Chain,Tx}=Message, #state{name=Name,storage=Storage}=State) ->
    Id = element(2,Tx),
    io:format("XA PREPARE: ~p~n",[{Tx}]),
    Val = try {ok,Op} = kvs:get(operation,Id),
%              replay(Storage,{prepare,Tx}),
              kvs:put(Op#operation{status=replayed})
       catch _:E ->
              io:format("PREPARE ERROR"),
              {error, E} end,
    Command = case [Chain,Val] of
        [_,A={rollback,_,_,_}] -> A;
                    [[Name],_] -> {commit,self(),cr:chain(Id),Tx};
                     [[H|T],_] -> {prepare,self(),T,Tx} end,
    continuation(H,Command,State);

handle_cast({commit,Sender,[H|T]=Chain,Tx}=Message, #state{name=Name,storage=Storage}=State) ->
    Id = element(2,Tx),
    io:format("XA COMMIT: ~p~n",[{Tx}]),
    Val = try {ok,Op} = kvs:get(operation,Id),
%              replay(Storage,Op),
              kvs:put(Op#operation{status=commited})
       catch _:E -> io:format("COMMIT ERROR"),
                    {error, E} end,
    Command = case [Chain,Val] of
        [_,A={rollback,_,_,_}] -> A;
                    [[Name],_] -> {nop,self(),[],[]};
                     [[H|T],_] -> {commit,self(),T,Tx} end,
    continuation(H,Command,State).

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

