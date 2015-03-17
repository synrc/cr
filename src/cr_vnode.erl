-module(cr_vnode).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-include_lib("kvs/include/kvs.hrl").
-include_lib("db/include/transaction.hrl").
-compile(export_all).
-record(state, {name,nodes}).
-export(?GEN_SERVER).

start_link(UniqueName,HashRing) ->
    gen_server:start_link(?MODULE, [UniqueName,HashRing], []).

init([UniqueName,HashRing]) ->
    error_logger:info_msg("VNODE PROTOCOL: started: ~p.~n",[UniqueName]),
    {ok,#state{name=UniqueName,nodes=HashRing}}.

handle_info({'EXIT', Pid,_}, #state{} = State) ->
    error_logger:info_msg("VNODE: EXIT~n",[]),
    {noreply, State};

handle_info(_Info, State) ->
    error_logger:info_msg("VNODE: Info ~p~n",[_Info]),
    {noreply, State}.

handle_call({transaction,Transaction},_,#state{name=Name}=Proc) ->
    supervisor:start_child(xa_sup,
                           cr_app:xa(Transaction#transaction.id,
                           Name,
                           Transaction)),
    {reply,Transaction#transaction.id,Proc};

handle_call(Request,_,Proc) ->
    error_logger:info_msg("VNODE: Call ~p~n",[Request]),
    {reply,ok,Proc}.

handle_cast(Msg, State) ->
    error_logger:info_msg("VNODE: Cast ~p", [Msg]),
    {stop, {error, {unknown_cast, Msg}}, State}.

terminate(_Reason, #state{}) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

