-module(provider_server).
-author('LiuFan <liufanyansha@sina.cn>').

-include("gen_rabbitmq_client.hrl").

-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {connections = []}).
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

publish(RoutingKey, Payload) ->
	publish(RoutingKey, Payload, []).
publish(RoutingKey, Payload, Opts) ->
	gen_server:call(?SERVER, {publish, RoutingKey, Payload, Opts}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Args) ->
	Config = [{rabbit1,
					{"192.168.22.132", 5672, {<<"guest">>, <<"guest">>}, <<"/">>},
			      	{[{<<"android">>, []}], 
			      	 [{<<"queue1">>, []}], 
			      	 [{<<"android">>, <<"queue1">>, <<"android.queue1">>}]
			      	}},
			  {rabbit2,
					{"192.168.22.132", 5672, {<<"guest">>, <<"guest">>}, <<"/">>},
			      	{[{<<"android">>, []}], 
			      	 [{<<"queue1">>, []}], 
			      	 [{<<"android">>, <<"queue1">>, <<"android.queue1">>}]
			      	}}],
	Connections = start_providers(Config, []),
    {ok, #state{connections = Connections}}.

handle_call({publish, RoutingKey, Payload, Opts}, _From, State = #state{connections = Connections}) ->
	Connection = gen_rabbitmq_client_util:get_one(Connections),
	Reply = gen_rabbitmq_client_provider:publish(Connection, RoutingKey, Payload, Opts),
	{reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

start_providers([], Connections) ->
	Connections;
start_providers([Config = {Name, _ConnectionInfo, _DeclareInfo}|Rest], Connections) ->
	{ok, _} = gen_rabbitmq_client_provider:start_link(Config),
	start_providers(Rest, [Name|Connections]).

