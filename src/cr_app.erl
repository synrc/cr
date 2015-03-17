-module(cr_app).
-copyright('Maxim Sokhatsky').
-include("rafter_opts.hrl").
-compile(export_all).

tcp(Name,Port,Mod,Nodes) -> {Name,{cr_tcp,start_link,[Name,Port,Mod,Nodes]},permanent,2000,worker,[cr_tcp]}.
log(Name,Nodes) -> {log,{cr_log,start_link,[Name,#rafter_opts{cluster=Nodes}]},permanent,2000,worker,[cr_log]}.
sup(SupName) -> {SupName,{supervisor,start_link,[{local,SupName},cr_connection,[]]},
                                 permanent,infinity,supervisor,[]}.

init([Nodes,Opts]) ->
    {ok, {{one_for_one, 5, 60}, lists:flatten([ protocol(O,Nodes) || O<-Opts ])
                                       ++ [ sup(vnode_sup), log(atom_to_list(node()),Nodes) ] }}.
stop(_)    -> ok.
start(_,_) ->
    {ok,Peers}=application:get_env(cr,peers),
    {N,P1,P2,P3}=lists:keyfind(node(),1,Peers),
    HashRing = {Partitions,VNodes} = cr_hash:fresh(4,node()),
    Sup = supervisor:start_link({local, cr_sup}, ?MODULE,
                [  Peers, [ { interconnect, P1, cr_interconnect },
                            { ping,         P2, cr_ping },
                            { client,       P3, cr_client } ]]),
    [ start_vnode({Index,Node},Peers) || {Index,Node} <- VNodes ],
    Sup.

protocol({Name,Port,Mod},Nodes) ->
  SupName = list_to_atom(lists:concat([Name,'_sup'])),
  [ tcp(Name,Port,Mod,Nodes),     % TCP listener gen_server
    sup(SupName)        ].           % Accepted Clients Supervisor

start_vnode(UniqueName,Nodes) ->
    Restart = permanent,
    Shutdown = 2000,
    {Index,NodeName} = UniqueName,
    ChildSpec = case Index of

        0 -> {UniqueName,{cr_rafter,start_link,
                [UniqueName,#rafter_opts{state_machine=cr_rafterback,cluster=Nodes}]},
                     Restart,Shutdown,worker,[cr_rafter]};

        _ -> {UniqueName,{cr_vnode, start_link,
                [UniqueName,Nodes]},
                     Restart,Shutdown,worker,[cr_vnode]}
    end,
    supervisor:start_child(vnode_sup,ChildSpec).
