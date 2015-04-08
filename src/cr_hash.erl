-module(cr_hash).
-description('Consistent Hash Ring').
-copyright('Synrc Research Center s.r.o.').
-compile(export_all).
-define(RINGTOP, trunc(math:pow(2,160)-1)). % SHA-1 space

% Our consistent ring hash module consists of five functions
% Why need we have more?

key_of(Object) -> crypto:hash(sha, term_to_binary(Object)).
inc(N) -> ?RINGTOP div N.
fresh(N, Seed) -> {N, [{Int,Seed} || Int <- lists:seq(0,(?RINGTOP-1),inc(N))]}.
succ(Idx,{N,Nodes}) -> <<Int:160/integer>> =Idx, {A,B}=lists:split((Int div inc(N))+1,Nodes), B++A.
