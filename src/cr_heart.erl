-module(cr_heart).
-description('Heart Monitor').
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-compile(export_all).
-record(state, {name,nodes,timers}).
-export(?GEN_SERVER).

start_link(Name,Nodes) ->
    gen_server:start_link(?MODULE, [Name,Nodes], []).

init([Name,Nodes]) ->

    Timers = [ begin
          [_,Addr]=string:tokens(atom_to_list(erlang:node()),"@"),
          {ok,Parsed}=inet:parse_address(Addr),
          Timer = erlang:send_after(1000,self(),{timer,ping,{Parsed,P2},Node,undefined}),
          {Node,Timer}
      end || {Node,_,P2,_}<-Nodes],

    error_logger:info_msg("HEART PROTOCOL: started: ~p~n"
                                   "Nodes: ~p~n",[Name,Timers]),

    {ok,#state{name=Name,nodes=Nodes,timers=Timers}}.

timer_restart(Diff,Connect,Node,Socket) ->
    {X,Y,Z} = Diff,
    erlang:send_after(1000*(Z+60*Y+60*60*X),self(),{timer,ping,Connect,Node,Socket}).

setkey(Name,Pos,List,New) ->
    case lists:keyfind(Name,Pos,List) of
        false -> [New|List];
        _Element -> lists:keyreplace(Name,Pos,List,New) end.

handle_info({'EXIT', Pid,_}, #state{} = State) ->
    error_logger:info_msg("HEART: EXIT~n",[]),
    {noreply, State};

handle_info({carrier,lost,N}, State=#state{timers=Timer}) ->
    error_logger:info_msg("HOST CARRIER LOST ~p~n",[N]),
    {noreply,State};

handle_info({timer,ping,{A,P},N,S}, State=#state{timers=Timers}) ->

    error_logger:info_msg("PING STATE: ~p~n",[{A,P,N,S}]),

    {N,Timer} = lists:keyfind(N,1,Timers),
    case Timer of undefined -> skip; _ -> erlang:cancel_timer(Timer) end,

    Socket = try gen_tcp:send(S,term_to_binary({ping})), S
           catch E:R -> case gen_tcp:connect(A,P,[{packet,0},{active,false}]) of
                             {ok,S1} -> gen_tcp:send(S1,term_to_binary({ping})), S1;
                             {error,_} -> undefined end end,

      Data = try case gen_tcp:recv(Socket,0) of
                      {error,_} -> {error,undefined};
                      {ok,PONG} when length(PONG) == 10 -> {ok,Socket} end
           catch E1:R1 ->    {error,recv} end,

    T = case Data of
         {error,_} -> timer_restart({0,0,5},{A,P},N,undefined);
         {ok,Sx}   -> timer_restart({0,0,5},{A,P},N,Sx) end,

    {noreply,State#state{timers=setkey(N,1,Timers,{N,T})}};

handle_info(_Info, State) ->
    error_logger:info_msg("HEART: Info ~p~n",[_Info]),
    {noreply, State}.

handle_call(Request,_,Proc) ->
    error_logger:info_msg("HEART: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast(Msg, State) ->
    error_logger:info_msg("HEART: Cast ~p", [Msg]),
    {stop, {error, {unknown_cast, Msg}}, State}.

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

