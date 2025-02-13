%% @doc Unit-tests for `iterator'.

-module(iterator_tests).

-include_lib("eunit/include/eunit.hrl").

pv_each_n_test() ->
    ForEachN = 5,
    I0 = iterator:from_list(lists:seq(1, 50)),
    Counter = counters:new(1, []),
    I1 = iterator:pv(
        fun(_, _Time, NItems, TotalItems) ->
            ok = counters:add(Counter, 1, ForEachN),
            ?assertEqual(TotalItems, counters:get(Counter, 1)),
            ?assertEqual(ForEachN, NItems)
        end,
        #{
            for_each_n => ForEachN,
            % large so it never triggers
            every_s => 120
        },
        I0
    ),
    iterator:to_list(I1).

%% XXX: ths test can be flaky because it relies on the sleep time
pv_every_s_test() ->
    EveryS = 1,
    Size = 70,
    Sleep = 50,
    ApproxPerBatch = (EveryS * 1000) div Sleep,
    I0 = iterator:from_list(lists:seq(1, Size)),
    I1 = iterator:map(
        fun(X) ->
            timer:sleep(Sleep),
            X
        end,
        I0
    ),
    I2 = iterator:pv(
        fun(_, Time, NItems, _TotalItems) ->
            TimeMs = erlang:convert_time_unit(Time, native, millisecond),
            %% We can't assert exact values here because of the sleep
            ?assert(abs(TimeMs - (EveryS * 1000)) < 30, [{time, TimeMs}]),
            ?assert(abs(NItems - ApproxPerBatch) < 4, [{n_items, NItems}]),
            io:format("ok!~n", [])
        end,
        #{
            % large so it never triggers
            for_each_n => 100,
            every_s => EveryS
        },
        I1
    ),
    iterator:to_list(I2).

%% @doc Test that with unlimited supply of items, with rate 1 and no burrst, we get 1 item per second
rate_token_bucket_flat_test() ->
    L = lists:seq(1, 5),
    Sleeps = [0, 0, 0, 0, 0],
    Times = [0, 500, 1000, 1500, 2000],
    I0 = iterator:from_list(L),
    I1 = iterator_rate:token_bucket(#{rate => 2, capacity => 1}, I0),
    ?assertEqual(L, test_rate(I1, Times, Sleeps)).

%% @doc Test that with unlimited supply of items, with rate 1, burst 5 we process first 5 items
%% immediately and then 1 item per second
rate_token_bucket_burst_test() ->
    L = lists:seq(1, 9),
    Sleeps = [0, 0, 0, 0, 0, 0, 0, 0, 0],
    Times = [0, 0, 0, 0, 0, 500, 1000, 1500, 2000],
    I0 = iterator:from_list(L),
    I1 = iterator_rate:token_bucket(#{rate => 2, capacity => 5}, I0),
    ?assertEqual(L, test_rate(I1, Times, Sleeps)).

%% @doc Test that with uneven consumption and no burst we just get a delay
rate_token_bucket_uneven_consumption_test() ->
    L = lists:seq(1, 5),
    Sleeps = [0, 0, 250, 0, 0],
    Times = [0, 500, 750, 1250, 1750],
    I0 = iterator:from_list(L),
    I1 = iterator_rate:token_bucket(#{rate => 2, capacity => 1}, I0),
    ?assertEqual(L, test_rate(I1, Times, Sleeps)).

%% @doc Test that with uneven supply we have a chance to refill the bucket
rate_token_bucket_uneven_consumption_burst_test() ->
    L = lists:seq(1, 9),
    Sleeps = [0, 0, 0, 500, 0, 0, 0, 0],
    Times = [0, 0, 0, 500, 500, 500, 1000, 1500],
    I0 = iterator:from_list(L),
    I1 = iterator_rate:token_bucket(#{rate => 2, capacity => 5}, I0),
    ?assertEqual(L, test_rate(I1, Times, Sleeps)).

%% @doc Delayed start just shifts the delay because burst is full anyway
rate_token_bucket_slow_start_burst_test() ->
    L = lists:seq(1, 9),
    Sleeps = [500, 0, 0, 0, 0, 0, 0, 0],
    Times = [500, 500, 500, 500, 500, 1000, 1500, 2000],
    I0 = iterator:from_list(L),
    I1 = iterator_rate:token_bucket(#{rate => 2, capacity => 5}, I0),
    ?assertEqual(L, test_rate(I1, Times, Sleeps)).

test_rate(I, Times, Sleeps) ->
    Start = erlang:monotonic_time(millisecond),
    test_rate(I, Times, Sleeps, Start).

test_rate(I, [T | Times], [S | Sleeps], Start) ->
    timer:sleep(S),
    {ok, Value, I1} = iterator:next(I),
    Now = erlang:monotonic_time(millisecond),
    assert_duration(T, Now - Start, 10),
    [Value | test_rate(I1, Times, Sleeps, Start)];
test_rate(I, [], [], _) ->
    %% Should return []
    iterator:to_list(I).

assert_duration(Expected, Duration, Tolerance) ->
    ?assert(
        abs(Expected - Duration) < Tolerance,
        #{expected => Expected, duration => Duration}
    ).
