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
    kvs:info(?MODULE,"VNODE: Info ~p~n",[_Info]),
    {noreply, State}.

kvs_replay(Operation, #state{storage=Storage}=State, Status) ->
    Storage:dispatch(Operation#operation.body,State),
    kvs:put(Operation#operation{status=Status}).

kvs_log({Cmd,Self,[{I,N}|T],Tx}=Message, #state{name=Name}=State) ->
    Id = element(2,Tx),
    kvs:info(?MODULE,"XA RECEIVE: ~p~n",[{Id,Message,Name}]),
    Operation = #operation{name=Cmd,body=Message,feed_id=Name,status=pending},
    {ok,Saved} = kvs:add(Operation#operation{id=kvs:next_id(operation,1)}),
    try gen_server:cast(self(),Saved)
    catch E:R -> kvs:info(?MODULE,"LOG ERROR ~p~n",[cr:stack(E,R)]) end.

continuation(Next,{_,_,[],Tx}=Command,State) -> {noreply, State};
continuation(Next,{C,S,[{I,N}|T],Tx}=Command,State) ->
    Id = element(2,Tx),
    Peer = cr:peer({I,N}),
    Vpid = cr:vpid({I,Peer}),
    case gen_server:cast(Vpid,{pending,Command}) of
                     ok -> kvs:info("XA SENT OK from ~p to ~p~n",[node(),Peer]), {noreply,State};
                  Error -> timer:sleep(1000),
                           continuation(Next,Command,State) end.

handle_call({pending,{Cmd,Self,[{I,N}|T],Tx}=Message}, _, #state{name=Name,storage=Storage}=State) ->
    kvs_log(Message,State),
    {reply, {ok,queued}, State};

handle_call(Request,_,Proc) ->
    kvs:info(?MODULE,"VNODE: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast({client,Client,Chain,Record}, #state{name=Name,storage=Storage}=State) ->
    {I,N} = hd(Chain),
    Self  = node(),
    gen_server:cast(case cr:peer({I,N}) of
                         Self -> cr:local(Record);
                         Node -> cr:vpid({I,Node}) end,
                    {pending,{prepare,Client,Chain,Record}}),
    {noreply, State};

handle_cast({pending,{Cmd,Self,[{I,N}|T],Tx}=Message}, #state{name=Name,storage=Storage}=State) ->
    kvs_log(Message,State),
    {noreply, State};

handle_cast(#operation{name=Command,body=Message}=Operation, #state{name=Name,storage=Storage}=State) ->
    {Command,Sender,[H|T]=Chain,Tx} = Message,
    Replay =   try cr_log:kvs_replay(node(),Operation,State,status(Command))
             catch E:R -> kvs:info(?MODULE,"~p REPLAY ~p~n",[code(Command),cr:stack(E,R)]),
                          {rollback, {E,R}, Chain, Tx} end,
    Forward = case [Chain,Replay] of
           [_,A={rollback,_,_,_}] -> A;
                       [[Name],_] -> last(Operation);
                        [[H|T],_] -> {Command,self(),T,Tx} end,
    try continuation(H,Forward,State)
    catch X:Y -> kvs:info(?MODULE,"~p SEND ~p~n",[code(Command),cr:stack(X,Y)]) end,
    {noreply,State}.

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

status(commit)   -> commited;
status(prepare)  -> prepared;
status(Unknown)  -> Unknown.

last(#operation{body={prepare,_,_,Tx}}) -> {commit,self(),cr:chain(element(2,Tx)),Tx};
last(#operation{body={commit,_,_,Tx}})  -> {nop,self(),[],[]}.

code(prepare)    -> "PREPARE";
code(commit)     -> "COMMIT";
code(rollback)   -> "ROLLBACK";
code(Unknown)    -> Unknown.
