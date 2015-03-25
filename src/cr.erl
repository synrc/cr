-module(cr).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-include("rafter.hrl").
-compile(export_all).

encode(Msg) -> term_to_binary(Msg).
decode(Bin) -> binary_to_term(Bin).

set_socket(Pid, Socket) when is_pid(Pid), is_port(Socket) ->
    gen_fsm:send_event(Pid, {socket_ready, Socket}).

send(Pid, Message) when is_pid(Pid)  ->
    gen_fsm:send_event(Pid, {out, Message}).

config() ->
    {ok,Peers} = application:get_env(cr,peers),
    N = lists:map(fun({N,_,_,_})->N end,Peers),
    #config{state=transitional,oldservers=N,newservers=N}.

secret() -> application:get_env(cr,secret,<<"ThisIsClassified">>).
peers() -> {ok,Peers}=application:get_env(cr,peers),Peers.
peers(N) -> lists:zip(lists:seq(1,N),lists:seq(1,N)).
hash(Object) -> hd(seq(Object)).
rep(Object) -> roll(element(2,hash(Object))).
chain(Object) -> lists:map(fun(X) -> lists:keyfind(X,2,cr:seq(Object)) end,cr:roll(element(2,cr:hash(Object)))).
roll(N) -> lists:seq(N,length(peers())) ++ lists:seq(1,N-1).
seq(Object) -> lists:keydelete(0,1,cr_hash:successors(cr_hash:key_of(Object),ring())).
ring() -> ring(4).
ring(C) -> {Nodes,[{0,1}|Rest]} = cr_hash:fresh(length(peers())*C,1),
           {Nodes,[{0,0}|lists:map(fun({{I,1},X})->{I,(X-1) div C+1} end,
                    lists:zip(Rest,lists:seq(1,length(Rest))))]}.
peer({I,N}) -> element(1,lists:nth(N,peers())).
