-module(cr_interconnect).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-compile(export_all).
-record(state, {succ,pred,port,name,socket,module}).

sup() -> interconnect_sup.

init([Name,Mod]) ->
    #state{name=Name,module=Mod}.

dispatch({'add_iterator',Object},State)  ->
    State;

dispatch({'get_container',Object},State)  ->
    State.

ring() -> cr_hash:fresh(5,node()).

list_replicas(Key) ->
    cr_hash:successors(cr_hash:key_of(Key),cr_hash:fresh(5,node())).
