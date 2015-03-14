-module(cr_client).
-include("cr.hrl").
-compile(export_all).
-record(state, {succ,pred,port,name,socket,module}).

sup() -> client_sup.

init([Name,Mod]) -> #state{name=Name,module=Mod}.

dispatch({'get',Args},State)  -> State;

dispatch({'put',Args},State)  -> State.
