-module(cr_app).
-behaviour(application).
-export([start/2, stop/1]).
-copyright('Maxim Sokhatsky').
-include("rafter_opts.hrl").
-compile(export_all).

tcp(Name,Port,Mod,Nodes) -> {Name,{cr_tcp,start_link,
                            [Name,Port,Mod,Nodes]},
                            permanent,2000,worker,[cr_tcp]}.

pool(SupName)            -> {SupName,{supervisor,start_link,
                            [{local,SupName},cr_connection,[]]},
                            permanent,infinity,supervisor,[]}.

vnode({I,N})             -> {I,{cr_vnode,start_link,
                            [{I,N},cr_kvs]},
                            permanent,2000,worker,[cr_vnode]}.

heart(Nodes)             -> {heart,{cr_heart,start_link,
                            ["heart",Nodes]},
                            permanent,2000,worker,[cr_heart]}.

log({I,N},Nodes)         -> {cr_log:logname(N),{cr_log,start_link,
                            [N,#rafter_opts{cluster=Nodes}]},
                            permanent,2000,worker,[cr_log]}.

rafter({I,N},Nodes)      -> {N,{cr_rafter,start_link,
                            [{I,N},#rafter_opts{state_machine=cr_replication,cluster=Nodes}]},
                            permanent,2000,worker,[cr_rafter]}.

init([Nodes,Opts]) ->
    {ok, {{one_for_one, 5, 60},
              lists:flatten([ log({0,N},Nodes)    || {N,_,_,_} <- Nodes, N == cr:node()]
                         ++ [ rafter({0,N},Nodes) || {N,_,_,_} <- Nodes, N == cr:node()]
                         ++ [ protocol(O,Nodes) || O<-Opts ]
                         ++ [ pool(heart_sup) ]
                         ++ [ pool(vnode_sup) ]) }}.

stop(_)    -> ok.
start() -> start(normal,[]).
start(_,_) ->
    io:format("Node: ~p~n",[cr:node()]),
    {ok,Peers}=application:get_env(cr,peers),
    {N,P1,P2,P3} = lists:keyfind(cr:node(),1,Peers),
    {_,VNodes} = cr:ring(),
    kvs:join(),
    Sup = supervisor:start_link({local, cr_sup}, ?MODULE,
                [  Peers, [ { interconnect, P1, cr_interconnect },
                            { ping,         P2, cr_ping },
                            { client,       P3, cr_client } ]]),
    io:format("Supervision: ~p~n",[supervisor:which_children(cr_sup)]),
    [ start_vnode({Index,Node},Peers) || {Index,Node} <- VNodes, Node == cr:nodex(cr:node()) ],
    spawn(fun() -> supervisor:start_child(heart_sup,heart(Peers)) end),
    Sup.

protocol({Name,Port,Mod},Nodes) ->
  SupName = list_to_atom(lists:concat([Name,'_',sup])),
  [ tcp(Name,Port,Mod,Nodes),     % TCP listener gen_server
    pool(SupName)        ].       % Accepted Clients Supervisor

start_vnode({0,_Name},Peers) -> skip;
start_vnode({Index,Name},_ ) -> supervisor:start_child(vnode_sup,vnode({Index,Name})).
