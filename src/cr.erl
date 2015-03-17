-module(cr).
-copyright('Maxim Sokhatsky').
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
