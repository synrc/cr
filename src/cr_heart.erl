-module(cr_heart).
-description('Heart Monitor').
-author('Maxim Sokhatsky').
-copyright('Synrc Research Center s.r.o.').
-include("cr.hrl").
-include("rafter.hrl").
-compile(export_all).
-record(state, {name,nodes,timers}).
-export(?GEN_SERVER).

%% Heart Monitor module is a single process, monitoring other cluster peers.
%% The Configuration of Ring is tracked by RAFT protocol and its log.

start_link(Name,Nodes) ->
    gen_server:start_link(?MODULE, [Name,Nodes], []).

init([Name,Nodes]) ->

    Timers = [ begin
          [_,Addr]=string:tokens(atom_to_list(cr:node()),"@"),
          {ok,Parsed}=inet:parse_address(Addr),
          Timer = erlang:send_after(1000,self(),{timer,ping,{Parsed,P2},Node,undefined}),
          {Node,Timer}
      end || {Node,_,P2,_}<-Nodes, Node /= cr:node()],

    cr:info(?MODULE,"HEART PROTOCOL: started: ~p~n"
                                   "Nodes: ~p~n",[Name,Timers]),

    {ok,#state{name=Name,nodes=Nodes,timers=Timers}}.

timer_restart(Diff,Connect,Node,Socket) ->
    {X,Y,Z} = Diff,
    erlang:send_after(1000*(1+Z+60*Y+60*60*X),self(),{timer,ping,Connect,Node,Socket}).

setkey(Name,Pos,List,New) ->
    case lists:keyfind(Name,Pos,List) of
        false -> [New|List];
        _Element -> lists:keyreplace(Name,Pos,List,New) end.

handle_info({'EXIT', Pid,_}, #state{} = State) ->
    cr:info(?MODULE,"HEART: EXIT~n",[]),
    {noreply, State};

handle_info({carrier,lost,N}, State=#state{timers=Timer}) ->
    cr:info(?MODULE,"HOST CARRIER LOST ~p~n",[N]),
    {noreply,State};

handle_info({timer,ping,{A,P},N,S}, State=#state{timers=Timers}) ->

    %kvs:info(?MODULE,"PING STATE: ~p~n",[{A,P,N,S}]),

    #config{newservers=Servers} = cr_log:get_config(cr:node()),

    {N,Timer} = lists:keyfind(N,1,Timers),
    case Timer of undefined -> skip; _ -> erlang:cancel_timer(Timer) end,

    Socket = try gen_tcp:send(S,term_to_binary({ping})), S
           catch E:R -> case gen_tcp:connect(A,P,[{packet,0},{active,false}]) of
                             {ok,S1} -> gen_tcp:send(S1,term_to_binary({ping})), S1;
                             {error,SErr} -> % io:format("Send Error: ~p~n",[{N,SErr}]),
                                             undefined end end,

      Data = try case gen_tcp:recv(Socket,0) of
                      {error,RErr} -> %io:format("Recv Error: ~p~n",[{N,RErr}]), 
                                      {error,undefined};
                      {ok,PONG} when length(PONG) == 10 -> {ok,Socket} end
           catch E1:R1 ->    {error,recv} end,

    {T,Operation,Online} = case Data of
         {error,_} -> {timer_restart({0,0,5},{A,P},N,undefined),remove,undefined};
         {ok,Sx}   -> {timer_restart({0,0,5},{A,P},N,Sx),add,Sx} end,

    case change(S,Online,N,Servers) of
         true ->
                 try
                      case cr_rafter:set_config(cr:node(),{N,Operation}) of
                           {error,_} -> skip;
                                  _ ->  cr:info(?MODULE,"Server Config Changed S/T ~p~n",
                                                         [{N,Operation}]) end
                 catch
                      _:Err -> cr:info(?MODULE,"CONFIG ERROR ~p~n",[Err]) end,
                 ok;
         false -> skip end,

    {noreply,State#state{timers=setkey(N,1,Timers,{N,T})}};

handle_info(_Info, State) ->
    cr:info(?MODULE,"HEART: Info ~p~n",[_Info]),
    {noreply, State}.

handle_call({heart},_,Proc) ->
    {reply,Proc,Proc};

handle_call(Request,_,Proc) ->
    cr:info(?MODULE,"HEART: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast(Msg, State) ->
    cr:info(?MODULE,"HEART: Cast ~p", [Msg]),
    {stop, {error, {unknown_cast, Msg}}, State}.

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

change(undefined,undefined,N,Servers) -> lists:member(N,Servers);
change(undefined,_,_,_) -> true;
change(_,undefined,_,_) -> true;
change(A,A,_,_) -> false;
change(_,_,_,_) -> true.
