-module(cr_xa).
-copyright('Maxim Sokhatsky').
-include_lib("kvs/include/kvs.hrl").
-include("cr.hrl").
-compile(export_all).
-record(state, {name,vnode}).
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

handle_call({prepare,Transaction}, State) ->
    io:format("XA PREPARE: ~p~n",[{Transaction}]),
    Val = try kvs:put(Transaction)
       catch _:E -> {error, E} end,
    {Val, State};

handle_call({commit, Transaction}, State) ->
    io:format("XA PREPARE: ~p~n",[{Transaction}]),
    Val = try kvs:link(Transaction)
       catch _:E -> {error, E} end,
    {reply, Val, State};

handle_call({rollback, Transaction}, State) ->
    io:format("XA PREPARE: ~p~n",[{Transaction}]),
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

