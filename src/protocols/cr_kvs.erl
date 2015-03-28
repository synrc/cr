-module(cr_kvs).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-compile(export_all).

dispatch({commit,_,_,Tx})  ->
    io:format("KVS LINK~n"),
    kvs:link(Tx);

dispatch({prepare,_,_,Tx})  ->
    io:format("KVS CREATE~n"),
    kvs:put(Tx);

dispatch({rollback,_,_,Tx})  ->
    io:format("KVS CLEAN~n"),
    kvs:delete(Tx);

dispatch(_)  -> ok.
