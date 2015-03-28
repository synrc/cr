-module(cr_vnode).
-description('Hash-ring Transaction Coordinator').
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-include_lib("kvs/include/kvs.hrl").
-include_lib("db/include/transaction.hrl").
-compile(export_all).
-record(state, {name,nodes,storage}).
-export(?GEN_SERVER).

start_link(UniqueName,Storage) ->
    gen_server:start_link(?MODULE, [UniqueName,Storage], []).

init([UniqueName,Storage]) ->
    [ gen_server:cast(UniqueName,Message) || #operation{body=Message} <-
       kvs:entries(kvs:get(log,{pending,UniqueName}),operation,udnefined) ],
    error_logger:info_msg("VNODE PROTOCOL: started: ~p.~n",[UniqueName]),
    {ok,#state{name=UniqueName,storage=Storage}}.

handle_info({'EXIT', Pid,_}, #state{} = State) ->
    error_logger:info_msg("VNODE: EXIT~n",[]),
    {noreply, State};

handle_info(_Info, State) ->
    error_logger:info_msg("VNODE: Info ~p~n",[_Info]),
    {noreply, State}.

quorum(A) -> {ok,A}.
replay(Storage,Message) -> Storage:dispatch(Message).

continuation(Next,{_,_,[],Tx}=Command,State) -> {noreply, State};
continuation(Next,{C,S,[{I,N}|T],Tx}=Command,State) ->
    Id = element(2,Tx),
    Peer = cr:peer({I,N}),
    Vpid = cr:vpid(I,Peer),
    io:format("{I:N:P:V}=~p~n",[{I,N,Peer,Vpid}]),
    case gen_server:call(Vpid,{pending,Command}) of
         {ok,Saved} -> io:format("CAST OK~n"), {noreply,State};
         {error,Reason} -> io:format("CAST ERROR~n"),
            timer:sleep(1000), continuation(Next,Command,State) end.

handle_call({pending,{Cmd,Self,[{I,N}|T],Tx}=Message}, _, #state{name=Name,storage=Storage}=State) ->
    Id = element(2,Tx),
    io:format("XA QUEUE: ~p~n",[{Id,Message,Name}]),
    Operation = #operation{id = kvs:next_id(operation,1),
                           body = Message,feed_id=Name,status=pending},
    Res  = cr_log:kvs_log(node(),Operation),
    This = self(), spawn(fun() -> gen_server:cast(This,Message) end),
    {reply,Res, State};

handle_call(Request,_,Proc) ->
    error_logger:info_msg("VNODE: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast({prepare,Sender,[H|T]=Chain,Tx}=Message, #state{name=Name,storage=Storage}=State) ->
    Id = element(2,Tx),
    io:format("XA PREPARE: ~p~n",[Id]),
    Val = try {ok,Op} = kvs:get(operation,Id),
              replay(Storage,Message),
              kvs:put(Op#operation{status=replayed})
       catch E:R ->
              io:format("PREPARE ~p ERROR ~p~n",[Storage,R]),
              io:format("~p~n",[cr:stack(E,R)]),
              {rollback, Sender, Chain, Tx} end,
    Command = case [Chain,Val] of
        [_,A={rollback,_,_,_}] -> A;
                    [[Name],_] -> {commit,self(),cr:chain(Id),Tx};
                     [[H|T],_] -> {prepare,self(),T,Tx} end,
    continuation(H,Command,State);

handle_cast({commit,Sender,[H|T]=Chain,Tx}=Message, #state{name=Name,storage=Storage}=State) ->
    Id = element(2,Tx),
    io:format("XA COMMIT: ~p~n",[Id]),
    Val = try {ok,Op} = kvs:get(operation,Id),
              replay(Storage,Message),
              kvs:put(Op#operation{status=commited})
       catch E:R -> io:format("COMMIT ~p ERROR ~p~n",[Storage,R]),
                    io:format("~p~n",[cr:stack(E,R)]),
                    {rollback,Sender,Chain,Tx} end,
    Command = case [Chain,Val] of
        [_,A={rollback,_,_,_}] -> A;
                    [[Name],_] -> {nop,self(),[],[]};
                     [[H|T],_] -> {commit,self(),T,Tx} end,
    continuation(H,Command,State).

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

