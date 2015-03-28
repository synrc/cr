-module(cr).
-copyright('Maxim Sokhatsky').
-include("cr.hrl").
-include_lib("db/include/transaction.hrl").
-include("rafter.hrl").
-compile(export_all).

encode(Msg) -> term_to_binary(Msg).
decode(Bin) -> binary_to_term(Bin).

set_socket(Pid, Socket) when is_pid(Pid), is_port(Socket) -> gen_fsm:send_event(Pid, {socket_ready, Socket}).
send(Pid, Message) when is_pid(Pid)  -> gen_fsm:send_event(Pid, {out, Message}).

config() ->
    {ok,Peers} = application:get_env(cr,peers),
    N = lists:map(fun({N,_,_,_})->N end,Peers),
    #config{state=transitional,oldservers=N,newservers=N}.

secret() -> application:get_env(cr,secret,<<"ThisIsClassified">>).
peers() -> {ok,Peers}=application:get_env(cr,peers),Peers.
peers(N) -> lists:zip(lists:seq(1,N),lists:seq(1,N)).
hash(Object) -> hd(seq(Object)).
rep(Object) -> roll(element(2,hash(Object))).
roll(N) -> lists:seq(N,length(peers())) ++ lists:seq(1,N-1).
seq(Object) -> lists:keydelete(0,1,cr_hash:successors(cr_hash:key_of(Object),ring())).
peer({I,N}) -> element(1,lists:nth(N,peers())).
nodex(Node) -> string:str(cr:peers(),[lists:keyfind(Node,1,cr:peers())]).
vpid({I,Node}) -> {I,P,_,_}=lists:keyfind(I,1,supervisor:which_children({vnode_sup,Node})), P.
local(Object) -> {I,N}=lists:keyfind(cr:nodex(node()),2,cr:chain(Object)),
                 {I,P,_,_}=lists:keyfind(I,1,supervisor:which_children(vnode_sup)), P.
ring() -> ring(4).

ring(C) -> {Nodes,[{0,1}|Rest]} = cr_hash:fresh(length(peers())*C,1),
           {Nodes,[{0,0}|lists:map(fun({{I,1},X})->{I,(X-1) div C+1} end,
                    lists:zip(Rest,lists:seq(1,length(Rest))))]}.

chain(Object) ->
    {N,_} = cr:ring(),
    lists:map(fun(X) -> lists:nth((X-1)*4+1,cr:seq(Object)) end,
              cr:roll(element(2,cr:hash(Object)))).

tx(Record) when is_tuple(Record) ->
    gen_server:cast(local(Record),{client,self(),chain(element(2,Record)),Record}).

stack(Error, Reason) ->
    Stacktrace = [case A of
         { Module,Function,Arity,Location} ->
             { Module,Function,Arity,proplists:get_value(line, Location) };
         Else -> Else end
    || A <- erlang:get_stacktrace()],
    [Error, Reason, Stacktrace].


error_page(Class,Error) ->
    io_lib:format("ERROR:  ~w:~w~n~n",[Class,Error]) ++
    "STACK: " ++
    [ io_lib:format("\t~w:~w/~w:~w\n",
        [ Module,Function,Arity,proplists:get_value(line, Location) ])
    ||  { Module,Function,Arity,Location} <- erlang:get_stacktrace() ].

test() -> test(10).
test(Num) ->
    O1 = lists:foldl(fun({_,_,_,A,_,_},Acc) -> A+Acc end,0,kvs:all(log)),
    T1 = length(kvs:all(transaction)),
    kvs:info(?MODULE,"Ops before: ~p~n",[O1]),
    [cr:tx(#transaction{id=kvs:next_id(transaction,1)})||I<-lists:seq(1,Num)],
    O2 = lists:foldl(fun({_,_,_,A,_,_},Acc) -> A+Acc end,0,kvs:all(log)),
    T2 = length(kvs:all(transaction)),
    kvs:info(?MODULE,"Ops after: ~p~n",[O2]).
