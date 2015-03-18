-module(cr_xa).
-copyright('Maxim Sokhatsky').
-include_lib("kvs/include/kvs.hrl").
-include("cr.hrl").
-compile(export_all).
-record(state, {name,storage,vnode,documents}).
-export(?GEN_SERVER).

start_link([UniqueName,VNode,Object]) ->
    gen_server:start_link(?MODULE, [UniqueName,VNode,Object], []).

init([UniqueName,VNode,Object]) ->
    error_logger:info_msg("XA TX: started: ~p.~n",[UniqueName]),
    {VNode,N} = cr:hash(Object), % accept only local VNODE TX requests
       Chain  = cr:rep(Object),
         Peer = cr:peer({VNode,N}),
       Actual = cr_log:get_config(node()),
    {ok,#state{name=UniqueName,storage=cr_kvs,vnode=VNode}}.

handle_info({'EXIT', Pid,_}, #state{} = State) ->
    error_logger:info_msg("VNODE: EXIT~n",[]),
    {noreply, State};

handle_info(_Info, State) ->
    error_logger:info_msg("VNODE: Info ~p~n",[_Info]),
    {noreply, State}.

quorum(A) -> {ok,A}.

continuation(Refer,Command) -> gen_server:call(Refer,Command).

handle_call({prepare,Refer,Chain,Tx}, _, #state{name=Name,storage=Storage}=State) ->
    io:format("XA PREPARE: ~p~n",[{Tx}]),
    Val = try Storage:dispatch({prepare,Tx})
       catch _:E -> {error, E} end,
    Command = case [Chain,Val] of
        [_,A={rollback,_,_,_}] -> A;
                    [[Name],_] -> {commit,Refer,cr:chain(Tx),Tx};
                     [[H|T],_] -> {prepare,Refer,T,Tx} end,
    continuation(Refer,Command),
    {reply, Val, State};

handle_call({commit,Refer,Chain,Tx}, _, #state{storage=Storage}=State) ->
    io:format("XA COMMIT: ~p~n",[{Tx}]),
    Val = try Storage:dispatch({commit,Tx})
       catch _:E -> {rollback,Refer,Chain,Tx} end,
    Command = case [Chain,Val] of
        [_,A={rollback,_,_,_}] -> A;
                      [Name,_] -> stop;
                     [[H|T],_] -> {commit,Refer,T,Tx} end,
    continuation(Refer,Command),
    {reply, Val, State};

handle_call({rollback,Refer,Chain,Tx}, _, #state{name=Name,storage=Storage}=State) ->
    io:format("XA ROLLBACK: ~p~n",[{Tx}]),
    Val = try Storage:dispatch({rollback,Tx})
       catch _:E -> io:format("XA ROLLBACK Error: ~p~n",[{E}]),
                    {error, E} end,
    Command = case Chain of
                  [Name] -> stop;
                   [H|T] -> {rollback,Refer,T,Tx} end,
    continuation(Refer,Command),
    {reply,Val,State};

handle_call(Request,_,Proc) ->
    error_logger:info_msg("VNODE: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast(Msg, State) ->
    error_logger:info_msg("VNODE: Cast ~p", [Msg]),
    {stop, {error, {unknown_cast, Msg}}, State}.

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
