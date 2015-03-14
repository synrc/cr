-module(cr).
-compile(export_all).

encode(Msg) -> term_to_binary(Msg).
decode(Bin) -> binary_to_term(Bin).
