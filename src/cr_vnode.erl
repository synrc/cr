-module(cr_vnode).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-compile(export_all).
-record(state, {name}).
-export(?GEN_SERVER).

start_vnode(UniqueName) ->
    Restart = permanent,
    Shutdown = 2000,
    ChildSpec = {UniqueName,{cr_vnode,start_link,[UniqueName]},Restart,Shutdown,worker,[cr_vnode]},
    supervisor:start_child(vnode_sup,ChildSpec).

start_link(UniqueName) ->
    gen_server:start_link(?MODULE, [UniqueName], []).

init([UniqueName]) ->
    error_logger:info_msg("VNODE PROTOCOL: started: ~p.~n",[UniqueName]),
    {ok,#state{name=UniqueName}}.

handle_info({'EXIT', Pid,_}, #state{} = State) ->
    error_logger:info_msg("VNODE: EXIT~n",[]),
    {noreply, State};

handle_info(_Info, State) ->
    error_logger:info_msg("VNODE: Info ~p~n",[_Info]),
    {noreply, State}.

handle_call(Request,_,Proc) ->
    error_logger:info_msg("VNODE: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast(Msg, State) ->
    error_logger:info_msg("VNODE: Cast ~p", [Msg]),
    {stop, {error, {unknown_cast, Msg}}, State}.

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

