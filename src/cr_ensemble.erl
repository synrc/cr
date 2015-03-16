-module(cr_ensemble).
-behaviour(riak_ensemble_backend).
-compile(export_all).
-include("cr.hrl").
-include_lib("riak_ensemble/include/riak_ensemble_types.hrl").

ensembles() ->
    riak_ensemble_state:ensembles(riak_ensemble_manager:get_cluster_state()).

create([H|T]=Peers) ->
    riak_ensemble_root:set_ensemble(tps_cluster,
        #ensemble_info{leader={1,node()}, views=[peers()], seq={0,0}, vsn={0,0},
                       mod=cr_ensemble, args=[]}).

-record(state, {
    ensemble   :: ensemble_id(),
    id         :: peer_id(),
    pos        :: integer(),
    name,
    proxy,      % gen_server
    async      :: pid()
}).

add_peers() ->
    riak_ensemble_peer:update_members(riak_ensemble_manager:get_leader_pid(root),
              [{add,Peer}||Peer<-cr_ensemble:peers()], 5000).

enable() -> riak_ensemble_manager:enable().

create() ->
%    {ok,[Known|_]} = riak_ensemble_manager:known_ensembles(),
%    {_,Ensemble} = Known,
    R = create(peers()),
    error_logger:info_msg("Ensemble: ~p~n",[R]),
    R.

join(Node) -> riak_ensemble_manager:join(Node,node()).

boot(N,Peers) ->
%  [ try riak_ensemble_manager:join(Node,node())
%       catch _:_ -> ok end||{Node,_,_,_}<-Peers, Node /= node() ],
    ok.

peers() -> [{2,'cr2@127.0.0.1'},
            {3,'cr3@127.0.0.1'}].

init(Ensemble, Id, []) ->
    error_logger:info_msg("ENSEMBLE INIT: ~p~n",[{Ensemble,Id}]),
    <<Hash:160/integer>> = riak_ensemble_util:sha(term_to_binary({Ensemble, Id})),
    Name = integer_to_list(Hash),
    #state{
        ensemble = Ensemble,
        id = Id,
        pos = Id,
        name = Name
    }.

new_obj(Epoch, Seq, ExtKey, Value) -> #ens{eseq=make_eseq(Epoch, Seq), key=ExtKey, val=Value}.
obj_epoch(#ens{eseq=ESeq}) -> {Epoch, _} = get_epoch_seq(ESeq), Epoch.
obj_seq(#ens{eseq=ESeq}) -> {_, Seq} = get_epoch_seq(ESeq), Seq.
obj_key(#ens{key=ExtKey}) -> ExtKey.
obj_value(#ens{val=Val}) -> Val.
set_obj_epoch(Epoch, #ens{eseq=ESeq}=EnsObj) -> {_, Seq} = get_epoch_seq(ESeq), EnsObj#ens{eseq=make_eseq(Epoch, Seq)}.
set_obj_seq(Seq, #ens{eseq=ESeq}=EnsObj) -> {Epoch, _} = get_epoch_seq(ESeq), EnsObj#ens{eseq=make_eseq(Epoch, Seq)}.
set_obj_value(Value, EnsObj) -> EnsObj#ens{val=Value}.
get(ExtKey, From, #state{proxy=Proxy}=State) -> catch Proxy ! {ensemble_get, ExtKey, From}, State.
put(ExtKey, EnsObj, From, #state{proxy=Proxy}=State) -> catch Proxy ! {ensemble_put, ExtKey, EnsObj, From}, State.
tick(_Epoch, _Seq, _Leader, Views, State) ->
    error_logger:info_msg("ENSEMBLE TICK:  Epoch: ~p~n"
                                        "    Seq: ~p~n"
                                        " Leader: ~p~n"
                                        "  Views: ~p~n"
                                        "  State: ~p~n",[_Epoch, _Seq, _Leader, Views, State]),
    State#state{}.

ping(From, #state{}=State) ->
    error_logger:info_msg("ENSEMBLE PING: ~p~n",[{}]),
    {async, State}.

handle_down(Ref, _Pid, Reason, #state{}=State) ->
    error_logger:info_msg("ENSEMBLE DOWN: ~p~n",[{}]),
    case Reason of
        normal -> ok;
        _ -> error_logger:info_msg("Crashed with reason: ~p",[Reason])
    end,
    {reset, State#state{}};

handle_down(_Ref, _Pid, _Reason, _State) -> false.

ready_to_start() ->
   error_logger:info_msg("Ready to start: ~p",[node()]),
    true.

synctree_path(E, Id) -> { <<0,(term_to_binary({E, Id}))/binary>>, lists:concat([cr,binary_to_list(term_to_binary({E,Id}))]) }.
make_eseq(Epoch, Seq) -> (Epoch bsl 32) + Seq.
get_epoch_seq(ESeq) -> <<Epoch:32/integer, Seq:32/integer>> = <<ESeq:64/integer>>, {Epoch, Seq}.
