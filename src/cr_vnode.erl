-module(cr_vnode).
-description('Ring Replica').
-author('Maxim Sokhatsky').
-copyright('Synrc Research Center s.r.o.').
-include("cr.hrl").
-include_lib("kvs/include/kvs.hrl").
-include_lib("db/include/transaction.hrl").
-compile(export_all).
-record(state, {name,nodes,storage,latency={inf,0,0,0}}). % latency {min,max,avg}
-export(?GEN_SERVER).

%% Ring Replica vnode is single point of execution inside CR DHT.
%% Each Node in Cluster has several replica vnodes.

start_link(Name,Storage) ->
    gen_server:start_link(?MODULE, [Name,Storage], []).

init([Name,Storage]) ->
    [ gen_server:cast(Name,O) || O <- kvs:entries(kvs:get(log,{pending,Name}),operation,-1) ],
    io:format("VNODE PROTOCOL: started: ~p.~n",[Name]),
    {ok,#state{name=Name,storage=Storage}}.

handle_info({'EXIT', Pid,_}, #state{} = State) ->
    io:format("VNODE: EXIT~n",[]),
    {noreply, State};

handle_info(_Info, State) ->
%    io:format("VNODE: Info ~p~n",[_Info]),
    {noreply, State}.

kvs_log({Cmd,Self,[{I,N}|T],Tx}=Message, #state{name=Name}=State) ->
    Id = element(2,Tx),
%    io:format("XA RECEIVE: ~p~n",[{Id,Message,Name}]),
    Operation = #operation{name=Cmd,body=Message,feed_id=Name,status=pending},
    {ok,Saved} = %kvs:add(Operation#operation{id=kvs:next_id(operation,1)}),
                 cr_log:kvs_log(cr:node(),Operation),
    try gen_server:cast(self(),Saved)
    catch E:R -> io:format("LOG ERROR ~p~n",[cr:stack(E,R)]) end.

continuation(Next,{_,_,[],Tx}=Command,State) -> {noreply, State};
continuation(Next,{C,S,[{I,N}|T],Tx}=Command,State) ->
    Id = element(2,Tx),
    Peer = cr:peer({I,N}),
    Vpid = cr:vpid({I,Peer}),
    case gen_server:cast(Vpid,{pending,Command}) of
                     ok -> % io:format("XA SENT OK from ~p to ~p~n",[cr:node(),Peer]),
                           {noreply,State};
                  Error -> timer:sleep(1000),
                           continuation(Next,Command,State) end.

handle_call({pending,{Cmd,Self,[{I,N}|T],Tx}=Message}, _, #state{name=Name,storage=Storage}=State) ->
    kvs_log(Message,State),
    {reply, {ok,queued}, State};

handle_call({latency},_,#state{latency={Min,Max,Avg,N}}=State) ->
    L = try X = {Min div 1000,Max div 1000,Avg div (N*1000)}
      catch _:_ -> {Min,Max,Avg} end,
    {reply,L,State};

handle_call(Request,_,Proc) ->
    io:format("VNODE: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast({client,Client,Chain,Record}, #state{name=Name,storage=Storage}=State) ->
    {I,N} = hd(Chain),
    Self  = cr:node(),
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
    Replay =   try cr_log:kvs_replay(cr:node(),Operation,State,status(Command))
             catch E:R -> %io:format("~p REPLAY ~p~n",[code(Command),cr:stack(E,R)]),
                          {rollback, {E,R}, Chain, Tx} end,
    {Forward,Latency} = case [Chain,Replay] of
           [_,A={rollback,_,_,_}] -> {A,State#state.latency};
                       [[Name],_] -> last(Operation,State);
                        [[H|T],_] -> {{Command,Sender,T,Tx},State#state.latency} end,
    try continuation(H,Forward,State)
    catch X:Y -> io:format("~p SEND ~p~n",[code(Command),cr:stack(X,Y)]) end,
    {noreply,State#state{latency=Latency}}.

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

status(commit)   -> commited;
status(prepare)  -> prepared;
status(Unknown)  -> Unknown.

% XA PROTOCOL
% last(#operation{body={prepare,{Sender,Time},_,Tx}},S) -> {{commit,{Sender,Time},cr:chain(element(2,Tx)),Tx},S#state.latency};
% last(#operation{body={commit,{Sender,Time},_,Tx}},S)  -> {{nop,{Sender,Time},[],[]},new_latency(Time,S)};

% CR PROTOCOL
last(#operation{body={_,{Sender,Time},_,Tx}},S)       -> {{nop,{Sender,Time},[],[]},new_latency(Time,S)}.

new_latency(Time,#state{latency=Latency}) ->
    {Min,Max,Avg,N} = Latency,
    L = time_diff(Time,os:timestamp()),
    {NMin,NMax} = case L of
         L when L > Max -> {Min,L};
         L when L < Min -> {L,Max};
                      _ -> {Min,Max} end,
    NAvg = Avg + L,
    {NMin,NMax,NAvg,N + 1}.

ms({Mega,Sec,Micro}) -> (Mega*1000000+Sec)*1000000+Micro.
time_diff(Now,Now2) -> ms(Now2) - ms(Now).

code(prepare)    -> "PREPARE";
code(commit)     -> "COMMIT";
code(rollback)   -> "ROLLBACK";
code(Unknown)    -> Unknown.
