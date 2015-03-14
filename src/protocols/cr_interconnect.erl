-module(cr_interconnect).
-include("cr.hrl").
-compile(export_all).
-record(state, {succ,pred,port,name,socket,module}).

sup() -> interconnect_sup.

init([Name,Mod]) -> #state{name=Name,module=Mod}.

dispatch({'',Object},State)  -> State;

dispatch({'event',Object},State)  -> State.
