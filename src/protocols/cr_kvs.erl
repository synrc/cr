-module(cr_kvs).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-compile(export_all).

dispatch({commit,Tx})  ->
    kvs:add(Tx);

dispatch({prepare,Tx})  ->
    kvs:put(Tx);

dispatch({rollback,Tx})  ->
    kvs:delete(Tx);

dispatch(_)  -> ok.
