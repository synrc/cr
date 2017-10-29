-module(cr_ping).
-copyright('Synrc Research Center s.r.o.').
-include("cr.hrl").
-compile(export_all).
-record(state, {port,name,socket,module,nodes}).

sup() -> ping_sup.

init([Name,Mod,Socket,Nodes]) -> #state{socket=Socket,nodes=Nodes}.

dispatch({'join',Object},State)  ->
    io:format("PING: Join request: ~p~n",[Object]),
    State;

dispatch({ping},#state{socket=Socket}=State)  ->
    io:format("PING: Message: ~p~n",[self()]),
    gen_tcp:send(Socket,term_to_binary({pong})),
    State;

dispatch({'leave',Object},State)  -> State;

dispatch(_,State)  -> State.
