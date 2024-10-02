%% @doc Parallel map implementation over iterator.
%%
%% It starts workers immediately, not on-demand. It kills workers when there is no more work or
%% as `close' cleanup.
%%
%% The `ordered' option controls if the order of elements in the resulting iterator should match
%% the order of input iterator.
%% - If `ordered' is `true', the order is guaranteed, but pool utilization might be not optimal if
%% the time to process single element is not uniform (head-of-line blocking).
%% - If `ordered' is `false', the order is not guaranteed, but pool utilization is optimal. Keep in
%% mind however that single worker can stay busy far beyond the `recv_timeout'.
%%
%% It prioritizes that all the workers are busy over returning the result immediately (so
%% it does not return a result untill all workers are busy or inner iterator is depleted).
%%
%% Worker processes are linked to the caller process. So it relies on link mechanism to kill
%% the workers in case of errors in map function. It is not recommended to catch
%% the error / trap exit and continue, because it may leave the workers hanging alive forever.
-module(iterator_pmap).

-export([
    pmap/2,
    pmap/3,
    flush/1
]).
-export([loop/3]).
-export_type([tag/0]).

-type tag() :: any().

-record(state, {
    state :: normal | final,
    recv_timeout :: timeout(),
    tag :: tag(),
    free :: [pid()],
    busy :: {ordered, queue:queue(pid())} | {unordered, sets:set(pid())},
    inner_i :: iterator:iterator(any()) | undefined
}).

%% @doc If your pmap has crashed and you had to catch the error, you can use this function to
%% flush the results from the workers. But it is recommended to not catch the error and crash.
%% If error is catched, there is a risk that workers are not killed.
-spec flush(tag()) -> [pid()].
flush(Tag) ->
    receive
        {Tag, Pid, _} ->
            [Pid | flush(Tag)]
    after 0 ->
        []
    end.

pmap(F, I) ->
    pmap(F, I, #{}).

%% @doc Parallel map over iterator.
%% @param F function to apply to each element of the input iterator (executed inside worker process)
%% @param I input iterator.
%% @param Opts options:
%%      <ul>
%%       <li>`concurrency' (default: 10) - number of workers; also, this number of items will be
%%        read-ahead from the input iterator</li>
%%       <li>`recv_timeout' (default: infinity) - timeout for receiving result from worker, if
%%       reached, the pool will be shut down and `timeout' error is generated</li>
%%       <li>`ordered' (default: true) - if `true', the order of elements in the resulting iterator
%%       will match the order of input iterator; otherwise order is not guaranteed, but worker
%%       pool utilization will be better if task size is not uniform</li>
%%       <li>`tag' (default: `erlang:make_ref()') - unique tag to identify the
%%       pool (see {@link flush/1}). Make sure it is small, because it is sent between processes
%%       a lot</li>
%%      </ul>
-spec pmap(
    fun((InType) -> OutType),
    iterator:iterator(InType),
    #{
        concurrency => pos_integer(),
        recv_timeout => timeout(),
        ordered => boolean(),
        tag => tag()
    }
) -> iterator:iterator(OutType) when
    InType :: any(),
    OutType :: any().
pmap(F, I, Opts) ->
    Concurrency = maps:get(concurrency, Opts, 10),
    RecvTimeout = maps:get(recv_timeout, Opts, infinity),
    Ordered = maps:get(ordered, Opts, true),
    Tag = maps:get(tag, Opts, erlang:make_ref()),
    Workers = launch(F, Tag, Concurrency),
    Busy = busy_init(Ordered),
    St = #state{
        state = normal,
        recv_timeout = RecvTimeout,
        tag = Tag,
        free = Workers,
        busy = Busy,
        inner_i = I
    },
    iterator:new(fun yield_next/1, St, fun shutdown/1).

yield_next(#state{state = normal, free = [_ | _]} = Pool) ->
    %% The inner iterator is not yet exhausted and there are free workers:
    %% push the next value to the worker as long as there are workers and values.
    yield_next(push(Pool));
yield_next(
    #state{state = normal, tag = Tag, free = [], busy = Busy, recv_timeout = Timeout} = Pool
) ->
    %% All workers are busy, wait for the next result.
    case recv(Busy, Tag, Timeout) of
        %% We can't get `empty' here, because we have at least one busy worker.
        {Result, Pid, Busy1} ->
            Pool1 = push(Pool#state{
                free = [Pid],
                busy = Busy1
            }),
            {Result, Pool1};
        timeout ->
            shutdown(Pool),
            error(timeout)
    end;
yield_next(#state{state = final, tag = Tag, busy = Busy, recv_timeout = Timeout} = Pool) ->
    %% The inner iterator is exhausted, wait for the remaining results.
    case recv(Busy, Tag, Timeout) of
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

busy_init(true) ->
    {ordered, queue:new()};
busy_init(false) ->
    {unordered, sets_new()}.

busy_push(Pid, {ordered, Queue}) ->
    {ordered, queue:in(Pid, Queue)};
busy_push(Pid, {unordered, Set}) ->
    {unordered, sets:add_element(Pid, Set)}.

busy_to_list({ordered, Queue}) ->
    queue:to_list(Queue);
busy_to_list({unordered, Set}) ->
    sets:to_list(Set).

-if(?OTP_RELEASE >= 24).
sets_new() -> sets:new([{version, 2}]).
-else.
sets_new() -> sets:new().
-endif.

push(#state{state = normal, tag = Tag, free = [Pid | Free], busy = Busy, inner_i = I} = Pool) ->
    case iterator:next(I) of
        {ok, Val, I1} ->
            Pid ! {Tag, self(), {next, Val}},
            push(Pool#state{
                free = Free,
                busy = busy_push(Pid, Busy),
                inner_i = I1
            });
        done ->
            %% TODO: shutdown `free` here
            Pool#state{state = final, inner_i = undefined}
    end;
push(#state{state = normal, free = []} = Pool) ->
    Pool.

recv({ordered, Busy}, Tag, Timeout) ->
    %% The order is guaranteed by using selective receive that matches by worker pid.
    case queue:out(Busy) of
        {{value, Pid}, Busy1} ->
            receive
                {Tag, Pid, {result, Result}} ->
                    {Result, Pid, {ordered, Busy1}}
            after Timeout ->
                timeout
            end;
        {empty, _} ->
            empty
    end;
recv({unordered, Busy}, Tag, Timeout) ->
    %% Order is not guaranteed because we receive first ready result
    case sets:is_empty(Busy) of
        true ->
            empty;
        false ->
            receive
                {Tag, Pid, {result, Result}} ->
                    {Result, Pid, {unordered, sets:del_element(Pid, Busy)}}
            after Timeout ->
                timeout
            end
    end.

%%
%% Pool management
%%

launch(F, Tag, Concurrency) ->
    [
        spawn_link(?MODULE, loop, [self(), F, Tag])
     || _ <- lists:seq(1, Concurrency)
    ].

shutdown(#state{free = Free, tag = Tag, busy = Busy}) ->
    Pids = Free ++ busy_to_list(Busy),
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
            {Tag, Pid, {result, _}} -> ok
        after 0 ->
            ok
        end
     || Pid <- Pids
    ],
    ok.

%% @private
loop(Parent, F, Tag) ->
    receive
        {Tag, Parent, {next, I}} ->
            Parent ! {Tag, self(), {result, F(I)}},
            ?MODULE:loop(Parent, F, Tag)
    end.
