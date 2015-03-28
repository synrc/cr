-module(cr_vnode).
-description('Hash-ring Transaction Coordinator').
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-include_lib("kvs/include/kvs.hrl").
-include_lib("db/include/transaction.hrl").
-compile(export_all).
-record(state, {name,nodes,storage}).
-export(?GEN_SERVER).

start_link(Name,Storage) ->
    gen_server:start_link(?MODULE, [Name,Storage], []).

init([Name,Storage]) ->
    [ gen_server:cast(Name,O) || O <- kvs:entries(kvs:get(log,{pending,Name}),operation,-1) ],
    kvs:info(?MODULE,"VNODE PROTOCOL: started: ~p.~n",[Name]),
    {ok,#state{name=Name,storage=Storage}}.

handle_info({'EXIT', Pid,_}, #state{} = State) ->
    kvs:info(?MODULE,"VNODE: EXIT~n",[]),
    {noreply, State};

handle_info(_Info, State) ->
    error_logger:info_msg("VNODE: Info ~p~n",[_Info]),
    {noreply, State}.

quorum(A) -> {ok,A}.

kvs_replay(Operation, #state{storage=Storage}=State, Status) ->
    Storage:dispatch(Operation#operation.body,State),
    kvs:put(Operation#operation{status=Status}).

continuation(Next,{_,_,[],Tx}=Command,State) -> {noreply, State};
continuation(Next,{C,S,[{I,N}|T],Tx}=Command,State) ->
    Id = element(2,Tx),
    Peer = cr:peer({I,N}),
    Vpid = cr:vpid(I,Peer),
    case gen_server:call(Vpid,{pending,Command}) of
             {ok,Saved} -> kvs:info("XA SENT OK from ~p to ~p~n",[node(),Peer]), {noreply,State};
         {error,Reason} -> kvs:info("XA SENDING ERROR: ~p~n",[Reason]),
                           timer:sleep(1000),
                           continuation(Next,Command,State) end.

handle_call({pending,{Cmd,Self,[{I,N}|T],Tx}=Message}, _, #state{name=Name,storage=Storage}=State) ->
    Id = element(2,Tx),
    kvs:info(?MODULE,"XA RECEIVE: ~p~n",[{Id,Message,Name}]),
    Operation = #operation{name=Cmd,body=Message,feed_id=Name,status=pending},
                  %cr_log:kvs_log(node(),Operation),
    This  = self(),
    spawn(fun() -> try gen_server:cast(This,Operation)
                 catch E:R -> kvs:info(?MODULE,"PENDING ASYNC ERROR ~p~n",[cr:stack(E,R)]) end end),
    {reply, {ok,queued}, State};

handle_call(Request,_,Proc) ->
    kvs:info(?MODULE,"VNODE: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast(#operation{name=prepare,body=Message}=Operation, #state{name=Name,storage=Storage}=State) ->
    {prepare,Sender,[H|T]=Chain,Tx} = Message,
    Id = element(2,Tx),
    kvs:info("XA PREPARE: ~p~n",[Id]),
    kvs:add(Operation#operation{id=kvs:next_id(operation,1)}),
    Val = try kvs_replay(Operation, State, replayed)
       catch E:R ->
              kvs:info("PREPARE ~p ERROR ~p~n",[Storage,R]),
              kvs:info("~p~n",[cr:stack(E,R)]),
              {rollback, {E,R}, Chain, Tx} end,
    Command = case [Chain,Val] of
        [_,A={rollback,_,_,_}] -> A;
                    [[Name],_] -> {commit,self(),cr:chain(Id),Tx};
                     [[H|T],_] -> {prepare,self(),T,Tx} end,
    spawn(fun() -> try continuation(H,Command,State)
                 catch X:Y -> kvs:info(?MODULE,"PREPARE ASYNC ERROR ~p~n",[cr:stack(X,Y)]) end end),
    {noreply,State};

handle_cast(#operation{name=commit,body=Message}=Operation, #state{name=Name,storage=Storage}=State) ->
    {commit,Sender,[H|T]=Chain,Tx} = Message,
    Id = element(2,Tx),
    kvs:info("XA COMMIT: ~p~n",[Id]),
    Val = try kvs_replay(Operation, State, commited)
       catch E:R -> kvs:info("COMMIT ~p ERROR ~p~n",[Storage,R]),
                    kvs:info("~p~n",[cr:stack(E,R)]),
                    {rollback,{E,R},Chain,Tx} end,
    Command = case [Chain,Val] of
        [_,A={rollback,_,_,_}] -> A;
                    [[Name],_] -> {nop,self(),[],[]};
                     [[H|T],_] -> {commit,self(),T,Tx} end,
    spawn(fun() -> try continuation(H,Command,State)
                 catch X:Y -> kvs:info("COMMIT ASYNC ERROR ~p~n",[cr:stack(X,Y)]) end end),
    {noreply,State};

handle_cast(#operation{name=rollback,body=Message}=Operation, #state{name=Name,storage=Storage}=State) ->
    {rollback,{E,R},[H|T]=Chain,Tx}=Message,
    Id = element(2,Tx),
    kvs:info("XA ROLLBACK: ~p~n"
                      "Id: ~p~n",[{E,R},Id]),
    {noreply, State}.

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
