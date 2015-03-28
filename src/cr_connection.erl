-module(cr_connection).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-behaviour(gen_fsm).
-compile(export_all).
-record(state, {port,name,socket,module,peer,state,nodes}).
-export(?GEN_FSM).
-export([listen/2, transfer/2]).
-define(TIMEOUT, 10000).

start_connection(Module,Socket,Nodes) ->
    {ok, {IP,Port}} = try inet:peername(Socket) catch _:_ -> {ok,{{127,0,0,1},now()}} end,
    Restart = permanent,
    Shutdown = 2000,
    UniqueName = {Module,IP,Port},
    ChildSpec = { UniqueName, { cr_connection, start_link, [UniqueName,Module,Socket,Nodes]},
        Restart, Shutdown, worker, [Module] },
    Sup = supervisor:start_child(Module:sup(),ChildSpec),
    kvs:info("SERVER: starting ~p listener: ~p~n",[Sup,{Module,IP,Port,Socket}]),
    Sup.

listen({socket_ready, Socket}, State) ->
    error_logger:error_msg("SERVER: Socket Ready ~p~n", [Socket]),
    inet:setopts(Socket, [{active, once}, {packet, 0}, binary]),
    {next_state, transfer, State#state{socket=Socket}, ?TIMEOUT};

listen(Other, State) ->
    error_logger:error_msg("SERVER: Unexpected message during listening ~p~n", [Other]),
    {next_state, listen, State, ?TIMEOUT}.

transfer({in,Binary}, #state{state=SubState,module=Module}=State) ->
%    error_logger:error_msg("SERVER: RECV ~p~n", [Binary]),
    NewSubState = Module:dispatch(cr:decode(Binary),SubState),
    {next_state, transfer, State#state{state=NewSubState}, ?TIMEOUT};

transfer({out,Message}, #state{socket=Socket,state=SubState}=State) ->
%    error_logger:error_msg("SERVER: SEND ~p~n", [Message]),
    Bytes = cr:encode(Message),
    gen_tcp:send(Socket, Bytes),
    {next_state, transfer, State, ?TIMEOUT};

transfer(timeout, State) ->
%    error_logger:error_msg("SERVER: Client connection timeout: ~p\n", [State]),
    {stop, normal, State};

transfer(_Data,  State) ->
    error_logger:error_msg("SERVER: unknown Data during transfer: ~p\n", [_Data]),
    {stop, normal, State}.

start_link(Name,Mod,Socket,Nodes) ->
    gen_fsm:start_link(?MODULE, [Name,Mod,Socket,Nodes], []).

init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1,
    MaxSecondsBetweenRestarts = 600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    {ok, {SupFlags, []}};

init([Name,Mod,Socket,Nodes]) ->
    error_logger:info_msg("PROTOCOL: starting ~p listener: ~p~n",[self(),{Name,Mod}]),
    process_flag(trap_exit, true),
    {ok,listen,#state{module=Mod,name=Name,
                      socket=Socket,nodes=Nodes,
                      state=Mod:init([Name,Mod,Socket,Nodes])}}.

handle_info({tcp, Socket, Bin}, StateName, #state{module=Module,state=SubState} = State) ->
    inet:setopts(Socket, [{active, once}]),
    ?MODULE:StateName({in,Bin}, State);

handle_info({tcp_closed,_S}, _, State) ->
    error_logger:info_msg("SERVER: TCP closed~n",[]),
    {stop, normal, State};

handle_info({'EXIT', Pid,_}, StateName, #state{} = State) ->
    error_logger:info_msg("SERVER: EXIT~n",[]),
    {next_state, StateName, State};

handle_info(_Info, StateName, State) ->
    error_logger:info_msg("SERVER: Info ~p~n",[_Info]),
    {noreply, StateName, State}.

handle_event(Event, StateName, State) -> {stop, {StateName, undefined_event, Event}, State}.
handle_sync_event(Event, _From, StateName, State) -> {stop, {StateName, undefined_event, Event}, State}.
terminate(_Reason, StateName, #state{socket=Socket}) -> gen_tcp:close(Socket).
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

