-module(cr_app).
-copyright('Maxim Sokhatsky').
-include("rafter_opts.hrl").
-compile(export_all).

tcp(Name,Port,Mod,Nodes) -> {Name,{cr_tcp,start_link,
                            [Name,Port,Mod,Nodes]},
                            permanent,2000,worker,[cr_tcp]}.

pool(SupName)            -> {SupName,{supervisor,start_link,
                            [{local,SupName},cr_connection,[]]},
                            permanent,infinity,supervisor,[]}.

vnode({I,N},Nodes)       -> {{I,N},{cr_vnode,start_link,
                            [{I,N},Nodes]},
                            permanent,2000,worker,[cr_vnode]}.

xa(Id)                   -> {Id,{cr_xa,start_link,
                            [Id]},
                            permanent,infinity,worker,[cr_xa]}.

log({I,N},Nodes)         -> {cr_log:logname(N),{cr_log,start_link,
                            [N,#rafter_opts{cluster=Nodes}]},
                            permanent,2000,worker,[cr_log]}.

rafter({I,N},Nodes)      -> {N,{cr_rafter,start_link,
                            [{I,N},#rafter_opts{state_machine=cr_replication,cluster=Nodes}]},
                            permanent,2000,worker,[cr_rafter]}.

init([Nodes,Opts]) ->
    {ok, {{one_for_one, 5, 60},
              lists:flatten([ protocol(O,Nodes) || O<-Opts ]
                         ++ [ pool(vnode_sup) ]
                         ++ [ log({0,N},Nodes)    || {N,_,_,_} <- Nodes]
                         ++ [ rafter({0,N},Nodes) || {N,_,_,_} <- Nodes]) }}.

stop(_)    -> ok.
start(_,_) ->
    {ok,Peers}=application:get_env(cr,peers),
    {N,P1,P2,P3}=lists:keyfind(node(),1,Peers),
    HashRing = {Partitions,VNodes} = cr_hash:fresh(8,node()),
    Sup = supervisor:start_link({local, cr_sup}, ?MODULE,
                [  Peers, [ { interconnect, P1, cr_interconnect },
                            { ping,         P2, cr_ping },
                            { client,       P3, cr_client } ]]),
    [ start_vnode({Index,Node},Peers) || {Index,Node} <- VNodes ],
    Sup.

protocol({Name,Port,Mod},Nodes) ->
  SupName = list_to_atom(lists:concat([Name,'_',sup])),
  [ tcp(Name,Port,Mod,Nodes),     % TCP listener gen_server
    pool(SupName)        ].       % Accepted Clients Supervisor

start_vnode({0,_Name},_Nodes) -> skip;
start_vnode({Index,Name},Nodes) -> supervisor:start_child(vnode_sup,vnode({Index,Name},Nodes)).
