-module(cr_xa).
-copyright('Maxim Sokhatsky').
-include_lib("kvs/include/kvs.hrl").
-include("cr.hrl").
-compile(export_all).
-record(state, {name,vnode,documents}).
-export(?GEN_SERVER).

start_link([UniqueName,VNode,Object]) ->
    gen_server:start_link(?MODULE, [UniqueName,VNode,Object], []).

init([UniqueName,VNode,Object]) ->
    error_logger:info_msg("XA TX: started: ~p.~n",[UniqueName]),
    {VNode,N} = cr:hash(Object), % accept only local VNODE TX requests
       Chain  = cr:rep(Object),
         Peer = cr:peer({VNode,N}),
       Actual = cr_log:get_config(node()),
    {ok,#state{name=UniqueName,vnode=VNode}}.

handle_info({'EXIT', Pid,_}, #state{} = State) ->
    error_logger:info_msg("VNODE: EXIT~n",[]),
    {noreply, State};

handle_info(_Info, State) ->
    error_logger:info_msg("VNODE: Info ~p~n",[_Info]),
    {noreply, State}.

quorum(A) -> {ok,A}.

handle_call({prepare,Refer,Chain,Tx}, _, #state{name=Name}=State) ->
    io:format("XA PREPARE: ~p~n",[{Tx}]),
    Val = try kvs:put(Tx)
       catch _:E -> {error, E} end,
    Command = case Chain of
                  [Name] -> {commit,Refer,cr:chain(Tx),Tx};
                   [H|T] -> {prepare,Refer,T,Tx} end,
    gen_server:call(Refer,Command),
    {reply, Val, State};

handle_call({commit,Refer,Chain,Tx}, _, State) ->
    io:format("XA COMMIT: ~p~n",[{Tx}]),
    Val = try kvs:add(Tx)
       catch _:E -> {error, E} end,
    Command = case Chain of
                  [Name] -> {Refer,stop};
                   [H|T] -> {commit,Refer,T,Tx} end,
    gen_server:call(Refer,Command),
    {reply, Val, State};

handle_call({rollback, Transaction}, _, State) ->
    io:format("XA ROLLBACK: ~p~n",[{Transaction}]),
    Val = try kvs:delete(Transaction)
       catch _:E -> {error, E} end,
    {reply,Val,State};

handle_call(Request,_,Proc) ->
    error_logger:info_msg("VNODE: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast(Msg, State) ->
    error_logger:info_msg("VNODE: Cast ~p", [Msg]),
    {stop, {error, {unknown_cast, Msg}}, State}.

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

