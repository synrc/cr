-module(cr_hash).
-compile(export_all).
-define(RINGTOP, trunc(math:pow(2,160)-1)).  % SHA-1 space
-include_lib("eunit/include/eunit.hrl").

-type cr_hash() :: {num_partitions(), [node_entry()]}.
%% A Node is the unique identifier for the owner of a given partition.
%% An Erlang Pid works well here, but the cr_hash module allows it to
%% be any term.
-type cr_hash_node() :: term().
%% Indices into the ring, used as keys for object location, are binary
%% representations of 160-bit integers.
-type index() :: <<_:160>>.
-type index_as_int() :: integer().
-type node_entry() :: {index_as_int(), cr_hash_node()}.
-type num_partitions() :: pos_integer().

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return true if named Node owns any partitions in the ring, else false.
-spec contains_name(Name :: cr_hash_node(), CHash :: cr_hash()) -> boolean().
contains_name(Name, CHash) ->
    {_NumPartitions, Nodes} = CHash,
    [X || {_,X} <- Nodes, X == Name] =/= [].

%% @doc Create a brand new ring.  The size and seednode are specified;
%%      initially all partitions are owned by the seednode.  If NumPartitions
%%      is not much larger than the intended eventual number of
%%       participating nodes, then performance will suffer.
-spec fresh(NumPartitions :: num_partitions(), SeedNode :: cr_hash_node()) -> cr_hash().
fresh(NumPartitions, SeedNode) ->
    Inc = ring_increment(NumPartitions),
    {NumPartitions, [{IndexAsInt, SeedNode} ||
           IndexAsInt <- lists:seq(0,(?RINGTOP-1),Inc)]}.

%% @doc Find the Node that owns the partition identified by IndexAsInt.
-spec lookup(IndexAsInt :: index_as_int(), CHash :: cr_hash()) -> cr_hash_node().
lookup(IndexAsInt, CHash) ->
    {_NumPartitions, Nodes} = CHash,
    {IndexAsInt, X} = proplists:lookup(IndexAsInt, Nodes),
    X.

sha(Bin) -> crypto:hash(sha, Bin).

%% @doc Given any term used to name an object, produce that object's key
%%      into the ring.  Two names with the same SHA-1 hash value are
%%      considered the same name.
-spec key_of(ObjectName :: term()) -> index().
key_of(ObjectName) ->    
    sha(term_to_binary(ObjectName)).

%% @doc Return all Nodes that own any partitions in the ring.
-spec members(CHash :: cr_hash()) -> [cr_hash_node()].
members(CHash) ->
    {_NumPartitions, Nodes} = CHash,
    lists:usort([X || {_Idx,X} <- Nodes]).

%% @doc Return a randomized merge of two rings.
%%      If multiple nodes are actively claiming nodes in the same
%%      time period, churn will occur.  Be prepared to live with it.
-spec merge_rings(CHashA :: cr_hash(), CHashB :: cr_hash()) -> cr_hash().
merge_rings(CHashA,CHashB) ->
    {NumPartitions, NodesA} = CHashA,
    {NumPartitions, NodesB} = CHashB,
    {NumPartitions, [{I,random_node(A,B)} || 
           {{I,A},{I,B}} <- lists:zip(NodesA,NodesB)]}.

%% @doc Given the integer representation of a cr_hash key,
%%      return the next ring index integer value.
-spec next_index(IntegerKey :: integer(), CHash :: cr_hash()) -> index_as_int().
next_index(IntegerKey, {NumPartitions, _}) ->
        Inc = ring_increment(NumPartitions),
        (((IntegerKey div Inc) + 1) rem NumPartitions) * Inc.

%% @doc Return the entire set of NodeEntries in the ring.
-spec nodes(CHash :: cr_hash()) -> [node_entry()].
nodes(CHash) ->
    {_NumPartitions, Nodes} = CHash,
    Nodes.

%% @doc Given an object key, return all NodeEntries in order starting at Index.
-spec ordered_from(Index :: index(), CHash :: cr_hash()) -> [node_entry()].
ordered_from(Index, {NumPartitions, Nodes}) ->
    <<IndexAsInt:160/integer>> = Index,
    Inc = ring_increment(NumPartitions),
    {A, B} = lists:split((IndexAsInt div Inc)+1, Nodes),
    B ++ A.

%% @doc Given an object key, return all NodeEntries in reverse order
%%      starting at Index.
-spec predecessors(Index :: index() | index_as_int(), CHash :: cr_hash()) -> [node_entry()].
predecessors(Index, CHash) ->
    {NumPartitions, _Nodes} = CHash,
    predecessors(Index, CHash, NumPartitions).
%% @doc Given an object key, return the next N NodeEntries in reverse order
%%      starting at Index.
-spec predecessors(Index :: index() | index_as_int(), CHash :: cr_hash(), N :: integer())
                  -> [node_entry()].
predecessors(Index, CHash, N) when is_integer(Index) ->
    predecessors(<<Index:160/integer>>, CHash, N);
predecessors(Index, CHash, N) ->
    Num = max_n(N, CHash),
    {Res, _} = lists:split(Num, lists:reverse(ordered_from(Index,CHash))),
    Res.

%% @doc Return increment between ring indexes given
%% the number of ring partitions.
-spec ring_increment(NumPartitions :: pos_integer()) -> pos_integer().
ring_increment(NumPartitions) ->
    ?RINGTOP div NumPartitions.

%% @doc Return the number of partitions in the ring.
-spec size(CHash :: cr_hash()) -> integer().
size(CHash) ->
    {_NumPartitions,Nodes} = CHash,
    length(Nodes).

%% @doc Given an object key, return all NodeEntries in order starting at Index.
-spec successors(Index :: index(), CHash :: cr_hash()) -> [node_entry()].
successors(Index, CHash) ->
    {NumPartitions, _Nodes} = CHash,
    successors(Index, CHash, NumPartitions).

%% @doc Given an object key, return the next N NodeEntries in order
%%      starting at Index.
-spec successors(Index :: index(), CHash :: cr_hash(), N :: integer())
                -> [node_entry()].
successors(Index, CHash, N) ->
    Num = max_n(N, CHash),
    Ordered = ordered_from(Index, CHash),
    {NumPartitions, _Nodes} = CHash,
    if Num =:= NumPartitions ->
	    Ordered;
       true ->
	    {Res, _} = lists:split(Num, Ordered),
	    Res
    end.

%% @doc Make the partition beginning at IndexAsInt owned by Name'd node.
-spec update(IndexAsInt :: index_as_int(), Name :: cr_hash_node(), CHash :: cr_hash())
            -> cr_hash().
update(IndexAsInt, Name, CHash) ->
    {NumPartitions, Nodes} = CHash,
    NewNodes = lists:keyreplace(IndexAsInt, 1, Nodes, {IndexAsInt, Name}),
    {NumPartitions, NewNodes}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
%% @doc Return either N or the number of partitions in the ring, whichever
%%      is lesser.
-spec max_n(N :: integer(), CHash :: cr_hash()) -> integer().
max_n(N, {NumPartitions, _Nodes}) ->
    erlang:min(N, NumPartitions).

%% @private
-spec random_node(NodeA :: cr_hash_node(), NodeB :: cr_hash_node()) -> cr_hash_node().
random_node(NodeA,NodeA) -> NodeA;
random_node(NodeA,NodeB) -> lists:nth(random:uniform(2),[NodeA,NodeB]).

%% ===================================================================
%% EUnit tests
%% ===================================================================

update_test() ->
    Node = 'old@host', NewNode = 'new@host',
    
    % Create a fresh ring...
    CHash = cr_hash:fresh(5, Node),
    GetNthIndex = fun(N, {_, Nodes}) -> {Index, _} = lists:nth(N, Nodes), Index end,
    
    % Test update...
    FirstIndex = GetNthIndex(1, CHash),
    ThirdIndex = GetNthIndex(3, CHash),
    {5, [{_, NewNode}, {_, Node}, {_, Node}, {_, Node}, {_, Node}, {_, Node}]} = update(FirstIndex, NewNode, CHash),
    {5, [{_, Node}, {_, Node}, {_, NewNode}, {_, Node}, {_, Node}, {_, Node}]} = update(ThirdIndex, NewNode, CHash).

contains_test() ->
    CHash = cr_hash:fresh(8, the_node),
    ?assertEqual(true, contains_name(the_node,CHash)),
    ?assertEqual(false, contains_name(some_other_node,CHash)).

max_n_test() ->
    CHash = cr_hash:fresh(8, the_node),
    ?assertEqual(1, max_n(1,CHash)),
    ?assertEqual(8, max_n(11,CHash)).
    
simple_size_test() ->
    ?assertEqual(8, length(cr_hash:nodes(cr_hash:fresh(8,the_node)))).

successors_length_test() ->
    ?assertEqual(8, length(cr_hash:successors(cr_hash:key_of(0),
                                            cr_hash:fresh(8,the_node)))).
inverse_pred_test() ->
    CHash = cr_hash:fresh(8,the_node),
    S = [I || {I,_} <- cr_hash:successors(cr_hash:key_of(4),CHash)],
    P = [I || {I,_} <- cr_hash:predecessors(cr_hash:key_of(4),CHash)],
    ?assertEqual(S,lists:reverse(P)).

merge_test() ->
    CHashA = cr_hash:fresh(8,node_one),
    CHashB = cr_hash:update(0,node_one,cr_hash:fresh(8,node_two)),
    CHash = cr_hash:merge_rings(CHashA,CHashB),
    ?assertEqual(node_one,cr_hash:lookup(0,CHash)).

