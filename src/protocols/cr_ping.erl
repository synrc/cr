-module(cr_ping).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-compile(export_all).
-record(state, {port,name,socket,module}).

sup() -> ping_sup.

init([Name,Mod]) -> #state{}.

dispatch({'join',Object},State)  ->
    error_logger:info_msg("PING: Join request: ~p~n",[Object]),
    State;

dispatch({'ping',Object},#state{socket=Socket}=State)  ->
    error_logger:info_msg("PING: Message: ~p in ~p~n",[Object,self()]),
    gen_tcp:send(Socket,term_to_binary({pong})),
    State;

dispatch({'leave',Object},State)  -> State.
