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
    HashRing = {Partitions,VNodes} = cr_hash:fresh(40,node()),
    Sup = supervisor:start_link({local, cr_sup}, ?MODULE,
                [HashRing,[ { interconnect, 9000, cr_interconnect },
                  { ping,         9001, cr_ping },
                  { client,       9002, cr_client } ]]),
    [ cr_vnode:start_vnode({Index,Node},HashRing) || {Index,Node} <- VNodes ],
    Sup.

protocol({Name,Port,Mod},HashRing) ->
  SupName = list_to_atom(lists:concat([Name,'_sup'])),
  [ tcp(Name,Port,Mod,HashRing),   % TCP listener gen_server
    sup(SupName)        ]. % Accepted Clients Supervisor
