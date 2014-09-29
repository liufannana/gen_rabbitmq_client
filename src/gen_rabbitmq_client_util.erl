-module(gen_rabbitmq_client_util).
-author('LiuFan <liufanyansha@sina.cn>').

-include("gen_rabbitmq_client.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-export([
         get_one/1]).

-export([new_message/1,
         get_payload/1,
         get_delivery_mode/1,
         set_delivery_mode/2,
         get_content_type/1,
         set_content_type/2]).

-export([new_queue/1]).

-export([new_exchange/1,
         new_exchange/2,
         get_type/1,
         set_type/2]).

-export([get_name/1,
         is_durable/1,
         set_durable/2]).

-export([connect/0, connect/1, declare/2]).
-export([declare_exchange/2, declare_queue/2, bind_queue/4]).


%% ------------------------------------------------------------------
%% get a random element
%% ------------------------------------------------------------------
get_one(List) ->
    RandomNum = random(length(List)),
    lists:nth(RandomNum, List).

random(Range) ->
    maybe_seed(),
    random:uniform(Range).

maybe_seed() ->
    case get(random_seed) of
        undefined -> random:seed(erlang:now());
        {X,X,X} -> random:seed(erlang:now());
        _ -> ok
    end.

%%
%% Connection and Declaration helpers.
%%

connect() ->
    connect(#amqp_params_network{}).

connect(Params) when is_record(Params, amqp_params_network) ->
    {ok, Connection} = amqp_connection:start(Params),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, {Connection, Channel}};
connect({Host}) ->
    connect(#amqp_params_network{host=Host});
connect({Host, Port}) ->
    connect(#amqp_params_network{host=Host, port=Port});
connect({Host, Port, {User, Pass}}) ->
    connect(#amqp_params_network{host=Host, port=Port,
                                   username=User,
                                   password=Pass});
connect({Host, Port, {User, Pass}, VHost}) ->
    connect(#amqp_params_network{host=Host, port=Port,
                                   username=User,
                                   password=Pass,
                                   virtual_host=VHost}).