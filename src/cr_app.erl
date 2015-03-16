-module(cr_app).
-copyright('Maxim Sokhatsky').
-compile(export_all).

tcp(Name,Port,Mod,HashRing) -> { Name,{cr_tcp,start_link,[Name,Port,Mod,HashRing]},permanent,2000,worker,[cr_tcp]}.
sup(SupName) -> { SupName,{supervisor,start_link,[{local,SupName},cr_connection,[]]},
                                 permanent,infinity,supervisor,[]}.

init([HashRing,Opts]) ->
    {ok, {{one_for_one, 5, 60}, lists:flatten([ protocol(O,HashRing) || O<-Opts ])
                                       ++ [ sup(vnode_sup) ] }}.
stop(_)    -> ok.
start(_,_) ->
    {ok,Peers}=application:get_env(cr,peers),
    {N,P1,P2,P3}=lists:keyfind(node(),1,Peers),
%    spawn(fun() -> cr_ensemble:boot(N,Peers) end),
    HashRing = {Partitions,VNodes} = cr_hash:fresh(40,node()),
    Sup = supervisor:start_link({local, cr_sup}, ?MODULE,
                [  Peers, [ { interconnect, P1, cr_interconnect },
                            { ping,         P2, cr_ping },
                            { client,       P3, cr_client } ]]),
    [ cr_vnode:start_vnode({Index,Node},Peers) || {Index,Node} <- VNodes ],
    Sup.

protocol({Name,Port,Mod},HashRing) ->
  SupName = list_to_atom(lists:concat([Name,'_sup'])),
  [ tcp(Name,Port,Mod,HashRing),   % TCP listener gen_server
    sup(SupName)        ]. % Accepted Clients Supervisor
