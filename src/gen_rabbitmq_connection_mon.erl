-module(gen_rabbitmq_connection_mon).
-author('LiuFan <liufanyansha@sina.cn>').
-behaviour(gen_server).

-include("gen_rabbitmq_client.hrl").

-define(SERVER, ?MODULE).
-define(CONNECT_TIMEOUT, timer:seconds(30)).
-define(RECONNECT_DELAY, timer:seconds(1)).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([
		start_link/0,
		start_link/1,
		connect/1,
		get_state/0
	]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% gen_server state
%% ------------------------------------------------------------------

-record(gen_rabbitmq_connection_mon, 
		{
			connections = dict:new(),
			connect_fun
		}).

-record(connection,
		{
			connection_pid,
			connection_info,
			connection_consumer
		}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
	start_link(fun gen_rabbitmq_client_util:connect/1).

start_link(ConnectFun) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [ConnectFun], []).

connect(ConnectionInfo) ->
	gen_server:call(?SERVER, {connect, self(), ConnectionInfo}, ?CONNECT_TIMEOUT).

get_state() ->
    gen_server:call(?SERVER, get_state).
%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([ConnectFun]) ->
    {ok, #gen_rabbitmq_connection_mon{connect_fun = ConnectFun}}.

handle_call(get_state, _From, State) ->
	{reply, {ok, State}, State};
handle_call({connect, ConsumerPid, ConnectionInfo}, From, State) ->
    case do_connect(ConsumerPid, ConnectionInfo, State) of
		{{ok, {ConnectionPid, ChannelPid}}, State1} ->
			{reply, {ok, {ConnectionPid, ChannelPid}}, State1};
		{{connection_error, Error}, State1} ->
            error_logger:error_report(
              ["Could not connect, scheduling reconnect.",
               {error, Error}]),

            NotifyOnConnect = fun(Pids) ->
                                      gen_server:reply(From, {ok, Pids})
                              end,

            schedule_reconnect(ConsumerPid, ConnectionInfo, NotifyOnConnect),
            {noreply, State1}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', ConnectionRef, process, _Object, _Info}, State) ->
    {Connection, State1} = remove_connection(ConnectionRef, State),

    ConsumerPid = Connection#connection.connection_consumer,
    ConnectionInfo = Connection#connection.connection_info,

    case is_process_alive(ConsumerPid) of
        true ->
            %% chnange the consumer state
            ConsumerPid ! {invalid, channel_pid},
            NotifyOnConnect = fun(Pids) ->
                                      ConsumerPid ! {reconnected, Pids}
                              end,

            schedule_reconnect(ConsumerPid, ConnectionInfo, NotifyOnConnect);
        false ->
            %% Don't schedule a reconnect if our ConsumerPid is down.
            error_logger:info_report(
              ["Missing consumer, not scheduling reconnect",
               {info, ConnectionInfo},
               {consumer_pid, ConsumerPid}])
    end,

    {noreply, State1};
handle_info({reconnect,
             ConsumerPid, ConnectionInfo,
             NotifyOnConnect},
            State) ->
    State2 = case do_connect(ConsumerPid, ConnectionInfo, State) of
                 {{ok, {ConnectionPid, ChannelPid}}, State1} ->
                     NotifyOnConnect({ConnectionPid, ChannelPid}),
                     State1;
                 {{connection_error, Error}, State1} ->
                     error_logger:error_report(
                       ["Could not connect, scheduling reconnect.",
                        {error, Error}]),
                     schedule_reconnect(
                       ConsumerPid, ConnectionInfo, NotifyOnConnect),
                     State1
             end,

    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
do_connect(ConsumerPid, ConnectionInfo,
           State=#gen_rabbitmq_connection_mon{connect_fun=ConnectFun}) ->
    try
        {ok, {ConnectionPid, ChannelPid}} = ConnectFun(ConnectionInfo),
        {{ok, {ConnectionPid, ChannelPid}}, add_connection(
                                              ConsumerPid, ConnectionPid,
                                              ConnectionInfo,
                                              State)}
    catch
        Type:What ->
            {{connection_error, {{Type, What, erlang:get_stacktrace()},
                                 {connection_info, ConnectionInfo}}}, State}
    end.

add_connection(ConsumerPid, ConnectionPid, ConnectionInfo,
               State=#gen_rabbitmq_connection_mon{connections=Connections}) ->
    ConnectionRef = erlang:monitor(process, ConnectionPid),

    State#gen_rabbitmq_connection_mon{
      connections=dict:store(ConnectionRef,
                             #connection{connection_consumer=ConsumerPid,
                                         connection_info=ConnectionInfo,
                                         connection_pid=ConnectionPid},
                             Connections)}.

schedule_reconnect(ConsumerPid, ConnectionInfo, NotifyOnConnect) ->
    timer:send_after(?RECONNECT_DELAY,
                     {reconnect,
                      ConsumerPid, ConnectionInfo, NotifyOnConnect}).

remove_connection(ConnectionRef,
                  State=#gen_rabbitmq_connection_mon{connections=Connections}) ->
    true = erlang:demonitor(ConnectionRef),

    {ok, Connection} = dict:find(ConnectionRef, Connections),
    NewConnections = dict:erase(ConnectionRef, Connections),

    {upgrade_connection(Connection), State#gen_rabbitmq_connection_mon{
                   connections=NewConnections}}.

upgrade_connection(Conn = #connection{}) ->
    Conn;
upgrade_connection({connection, ConsumerPid, ConnectionInfo}) ->
    #connection{connection_consumer=ConsumerPid,
                connection_info=ConnectionInfo}.