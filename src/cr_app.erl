-module(cr_app).
-copyright('Maxim Sokhatsky').
-compile(export_all).

tcp(Name,Port,Mod) -> { Name,{cr_tcp,start_link,[Name,Port,Mod]},permanent,2000,worker,[cr_tcp]}.
sup(SupName) -> { SupName,{supervisor,start_link,[{local,SupName},cr_connection,[]]},
                                 permanent,infinity,supervisor,[]}.

init(Opts) ->
    {ok, {{one_for_one, 5, 60}, lists:flatten([ protocol(O) || O<-Opts ])
                                       ++ [ sup(vnode_sup) ] }}.
stop(_)    -> ok.
start(_,_) ->
    Res = supervisor:start_link({local, cr_sup}, ?MODULE,
                [ { interconnect, 9000, cr_interconnect },
                  { ping,         9001, cr_ping },
                  { client,       9002, cr_client } ]),
    {Partitions,Nodes} = cr_hash:fresh(40,node()),
    [ cr_vnode:start_vnode(Node) || Node <- Nodes ],
    Res.

protocol({Name,Port,Mod}) ->
  SupName = list_to_atom(lists:concat([Name,'_sup'])),
  [ tcp(Name,Port,Mod),   % TCP listener gen_server
    sup(SupName)        ]. % Accepted Clients Supervisor
