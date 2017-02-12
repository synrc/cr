-module(cr_kvs).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-compile(export_all).

dispatch({prepare,_,_,Tx}, {state,Name,_,_,_})  ->
    cr:info(?MODULE,"KVS PUT ~p:~p~n",[element(1,Tx),element(2,Tx)]),
    kvs:put(Tx);

dispatch({commit,_,_,Tx}, {state,Name,_,_,_})  ->
    cr:info(?MODULE,"KVS LINK ~p:~p~n",[element(1,Tx),element(2,Tx)]),
    kvs:link(Tx);

dispatch({rollback,_,_,Tx}, {state,Name,_,_,_})  ->
    cr:info(?MODULE,"KVS REMOVE ~p:~p~n",[element(1,Tx),element(2,Tx)]),
    kvs:remove(Tx);

dispatch(_,_)  -> ok.
