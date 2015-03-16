-module(cr_ensemble).
-behaviour(riak_ensemble_backend).
-compile(export_all).
-include("cr.hrl").
-export([init/3, new_obj/4]).
-export([obj_epoch/1, obj_seq/1, obj_key/1, obj_value/1]).
-export([set_obj_epoch/2, set_obj_seq/2, set_obj_value/2]).
-export([get/3, put/4, tick/5, ping/2, ready_to_start/0]).
-export([synctree_path/2]).
-export([handle_down/4]).

-include_lib("riak_ensemble/include/riak_ensemble_types.hrl").

create(Ensemble,Peers) ->
    riak_ensemble_manager:create_ensemble(Ensemble, undefined, Peers, cr_ensemble, []).

-record(state, {
    ensemble   :: ensemble_id(),
    id         :: peer_id(),
    pos        :: integer(),
    name,
    proxy      :: atom(),
    proxy_mon  :: reference(),
    vnode_mon  :: reference(),
    async      :: pid()
}).

boot(Peers) ->
  [ try riak_ensemble_manager:join(Node,node())
       catch _:_ -> ok end||{Node,_,_,_}<-Peers, Node /= node() ],

    riak_ensemble_manager:enable(),
    Known = case riak_ensemble_manager:known_ensembles() of
        {ok, EnsList} -> [Name || {Name, _Info} <- EnsList];
        _ -> []
    end,
    Ensemble = hd(Known),
    create(Ensemble, peers()),
    riak_ensemble_peer:update_members(riak_ensemble_manager:get_leader_pid(root),
      [{add,Peer}||Peer<-cr_ensemble:peers()], 5000),
    error_logger:info_msg("Ensemble: ~p~n",[Ensemble]),
    ok.


peers() -> [{1,'cr@127.0.0.1'},
            {2,'cr2@127.0.0.1'},
            {3,'cr3@127.0.0.1'}].

init(Ensemble, Id, []) ->
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
    error_logger:info_msg("ENSEMBLE TICK: ~p~n",[{_Epoch, _Seq, _Leader, Views, State}]),
    #state{
        id = {{nkv, EnsIdx, N, _}, _},
        async = Async
    } = State,
    Latest = hd(Views),
    Peers = peers(),
    Add = Peers -- Latest,
    Del = Latest -- Peers,
    Changes = [{add, Peer} || Peer <- Add] ++ [{del, Peer} || Peer <- Del],
    case Changes of
        [] ->
            State;
        _ ->
            case is_pid(Async) andalso is_process_alive(Async) of
                true ->
                    State;
                false ->
                    Self = self(),
                    Async2 = spawn(
                        fun() ->
                            case riak_ensemble_peer:update_members(Self, Changes, 5000) of
                                ok ->
                                    lager:warning("Executed Ensemble Changes at ~p~n",[Changes]);
                                _ ->
                                    error
                            end
                        end),
                        State#state{}
            end
    end.

ping(From, #state{proxy=Proxy}=State) -> catch Proxy ! {ensemble_ping, From}, {async, State}.

handle_down(Ref, _Pid, Reason, #state{}=State) ->
    #state{
        id = {{nkv, EnsIdx, _N, Idx}, _},
        pos = Pos
    } = State,
    case Reason of
        normal -> ok;
        _ -> lager:warning("Ensemble Backend vnode crashed with reason: ~p",[Reason])
    end,
    {reset, State#state{}};

handle_down(Ref, _Pid, Reason, #state{}=State) ->
    #state{
        id = {{nkv, EnsIdx, _N, _Idx}, _},
        proxy = Proxy, 
        pos=Pos
    } = State,
    case Reason of
        normal ->
            ok;
        _ ->
            lager:warning("Ensemble Backend vnode proxy crashed with reason: ~p",[Reason])
    end,
    {reset, State#state{}};

handle_down(_Ref, _Pid, _Reason, _State) -> false.

ready_to_start() -> true.

synctree_path(_Ensemble, Id) ->
    {{nkv, EnsIdx, N, Idx}, _} = Id,
    Bin = term_to_binary({EnsIdx, N}),
    TreeId = <<0, Bin/binary>>,
    {TreeId, "nkv_" ++ integer_to_list(Idx)}.

make_eseq(Epoch, Seq) -> (Epoch bsl 32) + Seq.
get_epoch_seq(ESeq) -> <<Epoch:32/integer, Seq:32/integer>> = <<ESeq:64/integer>>, {Epoch, Seq}.
