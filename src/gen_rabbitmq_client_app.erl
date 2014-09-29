-module(gen_rabbitmq_client_app).
-author('LiuFan <liufanyansha@sina.cn>').
-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    gen_rabbitmq_client_sup:start_link().

stop(_State) ->
    ok.
