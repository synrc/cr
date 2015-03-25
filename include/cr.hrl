-define(GEN_SERVER, [init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-define(GEN_FSM,[init/1, handle_event/3, handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

-include_lib("kvs/include/kvs.hrl").

-type mode() :: active | pending | immutable | sync.

-record(ens, {eseq,key,val}).


