-module(cr_interconnect).
-copyright('Synrc Research Center s.r.o.').
-include("cr.hrl").
-compile(export_all).
-record(state, {succ,pred,port,name,socket,module,nodes}).

sup() -> interconnect_sup.
init([Name,Mod,Socket,Nodes]) -> #state{name=Name,module=Mod,socket=Socket,nodes=Nodes}.

reply(Socket,Result,State) -> gen_tcp:send(Socket,term_to_binary(Result)), State.

dispatch({Command,Object},#state{socket=Socket}=State) ->
    io:format("CONS {_,_} VNODE command: ~p~n",[{Object}]),
    reply(Socket,gen_server:call(cr:peer(cr:hash(Object)),{Command,Object}),State);

dispatch({Command,Tx,Transaction}, #state{name=Name,socket=Socket}=State) ->
    io:format("CONS {_,_,_} XA command: ~p~n",[{Transaction}]),
    reply(Socket,gen_server:call(element(2,Tx),{Command,Transaction}),State);

dispatch(_,State)  -> State.
