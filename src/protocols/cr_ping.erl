-module(cr_ping).
-include("cr.hrl").
-compile(export_all).
-record(state, {succ,pred,port,name,socket,module}).

sup() -> ping_sup.

init([Name,Mod]) -> #state{}.

dispatch({'join',Object},State)  ->
    error_logger:info_msg("PING: Join request: ~p~n",[Object]),
    State;

dispatch({'ping',Object},State)  ->
    error_logger:info_msg("PING: Message: ~p in ~p~n",[Object,self()]),
    State;

dispatch({'pong',Object},State)  -> State.
