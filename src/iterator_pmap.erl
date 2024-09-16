%% @doc Parallel map implementation over iterator.
%%
%% It starts workers immediately, not on-demand. It kills workers when there is no more work or
%% as `close' cleanup.
%%
%% While input is processed in parallel, the original order is preserved in the output.
%%
%% It prioritizes that all the workers are busy over returning the result immediately (so
%% it does not return a result untill all workers are busy or inner iterator is depleted).
%%
%% When recv_timeout is not infinity, a `timeout' exception may be raised and no worker
%% cleanup is done. It is recommended to not catch this error and crash the calling process.
%%
%% Worker processes are linked to the caller process.
-module(iterator_pmap).

-export([
    pmap/2,
    pmap/3,
    flush/0
]).
-export([loop/2]).

-record(state, {
    state :: normal | final,
    recv_timeout :: timeout(),
    free :: [pid()],
    busy :: queue:queue(pid()),
    inner_i :: iterator:iterator(any()) | undefined
}).

%% @doc If your pmap has crashed and you had to catch the error, you can use this function to
%% flush the results from the workers. But it is recommended to not catch the error and crash.
%% If error is catched, there is a risk that workers are not killed.
-spec flush() -> [pid()].
flush() ->
    receive
        {?MODULE, Pid, _} ->
            [Pid | flush()]
    after 0 ->
        []
    end.

pmap(F, I) ->
    pmap(F, I, #{}).

-spec pmap(
    fun((InType) -> OutType),
    iterator:iterator(InType),
    #{
        concurrency => pos_integer(),
        recv_timeout => timeout()
    }
) -> iterator:iterator(OutType) when
    InType :: any(),
    OutType :: any().
pmap(F, I, Opts) ->
    Concurrency = maps:get(concurrency, Opts, 10),
    RecvTimeout = maps:get(recv_timeout, Opts, infinity),
    Workers = launch(F, Concurrency),
    St = #state{
        state = normal,
        recv_timeout = RecvTimeout,
        free = Workers,
        busy = queue:new(),
        inner_i = I
    },
    iterator:new(fun yield_next/1, St, fun shutdown/1).

yield_next(#state{state = normal, free = [_ | _]} = Pool) ->
    %% The inner iterator is not yet exhausted and there are free workers:
    %% push the next value to the worker as long as there are workers and values.
    yield_next(push(Pool));
yield_next(#state{state = normal, free = [], busy = Busy, recv_timeout = Timeout} = Pool) ->
    %% All workers are busy, wait for the next result.
    {Result, Pid, Busy1} = recv(Busy, Timeout),
    Pool1 = push(Pool#state{
        free = [Pid],
        busy = Busy1
    }),
    {Result, Pool1};
yield_next(#state{state = final, busy = Busy, recv_timeout = Timeout} = Pool) ->
    %% The inner iterator is exhausted, wait for the remaining results.
    case recv(Busy, Timeout) of
        {Result, Pid, Busy1} ->
            true = unlink(Pid),
            exit(Pid, shutdown),
            {Result, Pool#state{busy = Busy1}};
        timeout ->
            shutdown(Pool),
            error(timeout);
        empty ->
            done
    end.

%%
%% Pool API
%%

push(#state{state = normal, free = [Pid | Free], busy = Busy, inner_i = I} = Pool) ->
    case iterator:next(I) of
        {ok, Val, I1} ->
            Pid ! {?MODULE, self(), {next, Val}},
            push(Pool#state{
                free = Free,
                busy = queue:in(Pid, Busy),
                inner_i = I1
            });
        done ->
            Pool#state{state = final, inner_i = undefined}
    end;
push(#state{state = normal, free = []} = Pool) ->
    Pool.

recv(Busy, Timeout) ->
    case queue:out(Busy) of
        {{value, Pid}, Busy1} ->
            receive
                {?MODULE, Pid, {result, Result}} ->
                    {Result, Pid, Busy1}
            after Timeout ->
                timeout
            end;
        {empty, _} ->
            empty
    end.

%%
%% Pool management
%%

launch(F, Concurrency) ->
    [
        spawn_link(?MODULE, loop, [self(), F])
     || _ <- lists:seq(1, Concurrency)
    ].

shutdown(#state{free = Free, busy = Busy}) ->
    Pids = Free ++ queue:to_list(Busy),
    lists:foreach(
        fun(Pid) ->
            unlink(Pid),
            exit(Pid, shutdown)
        end,
        Pids
    ),
    %% Flush potential `result' messages from workers
    [
        receive
            {?MODULE, Pid, {result, _}} -> ok
        after 0 ->
            ok
        end
     || Pid <- Pids
    ],
    ok.

loop(Parent, F) ->
    receive
        {?MODULE, Parent, {next, I}} ->
            Parent ! {?MODULE, self(), {result, F(I)}},
            ?MODULE:loop(Parent, F)
    end.
