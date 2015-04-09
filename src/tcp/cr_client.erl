-module(cr_client).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-compile(export_all).
-record(state, {succ,pred,port,name,socket,module,nodes}).

sup() -> client_sup.

init([Name,Mod,Socket,Nodes]) -> #state{name=Name,module=Mod,socket=Socket,nodes=Nodes}.

dispatch({transaction,Object},#state{nodes=Nodes,name=Name,socket=Socket}=State) ->
    Result = cr:tx(Object),
    gen_tcp:send(Socket,term_to_binary(Result)),
    State;

dispatch(_,State) -> State.
