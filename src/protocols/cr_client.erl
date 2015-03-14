-module(cr_client).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-compile(export_all).
-record(state, {succ,pred,port,name,socket,module}).

sup() -> client_sup.

init([Name,Mod,Socket]) -> #state{name=Name,module=Mod,socket=Socket}.

dispatch({'get',Args},State)  -> State;

dispatch({'transaction',Args},State) -> State.
