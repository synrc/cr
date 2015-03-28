-module(cr_kvs).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-compile(export_all).

dispatch({prepare,_,_,Tx}, {state,Name,_,_})  ->
    io:format("KVS CREATE~n"),
    kvs:put(Tx);

dispatch({commit,_,_,Tx}, {state,Name,_,_})  ->
    io:format("KVS LINK~n"),
    kvs:link(Tx);

dispatch({rollback,_,_,Tx}, {state,Name,_,_})  ->
    io:format("KVS CLEAN~n"),
    kvs:delete(Tx);

dispatch(_,_)  -> ok.
