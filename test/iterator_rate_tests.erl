-module(iterator_rate_tests).

-include_lib("eunit/include/eunit.hrl").

%%
%% Token bucket tests
%%

%% @doc Test that with unlimited supply of items, with rate 2 and no burrst, we get 2 per second
rate_token_bucket_flat_test() ->
    L = lists:seq(1, 5),
    Sleeps = [0, 0, 0, 0, 0],
    Times = [0, 500, 1000, 1500, 2000],
    I0 = iterator:from_list(L),
    I1 = iterator_rate:token_bucket(#{rate => 2, capacity => 1}, I0),
    ?assertEqual(L, test_rate(I1, Times, Sleeps)).

%% @doc Test that with unlimited supply of items, with rate 2, burst 5 we process first 5 items
%% immediately and then 2 per second
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

%% @doc Test that consumer which is slower than the rate limiter would not be throttled
rate_token_bucket_slow_consumer_test() ->
    L = lists:seq(1, 5),
    Sleeps = [500, 500, 500, 500, 500],
    Times = [500, 1000, 1500, 2000, 2500],
    I0 = iterator:from_list(L),
    % high rate
    I1 = iterator_rate:token_bucket(#{rate => 10, capacity => 1}, I0),
    ?assertEqual(L, test_rate(I1, Times, Sleeps)).

%%
%% Leaky bucket tests - Comprehensive suite
%%

%% @doc Test baseline: steady rate limiting with no delays
%% Rate: 10/sec (100ms per item), Items: 10
%% Expected: 0, 100, 200, ... 900ms (first item immediate, then regular intervals)
%% Runtime: ~900ms
rate_leaky_bucket_baseline_test() ->
    L = lists:seq(1, 10),
    Sleeps = lists:duplicate(10, 0),
    Times = [N * 100 || N <- lists:seq(0, 9)],
    I0 = iterator:from_list(L),
    I1 = iterator_rate:leaky_bucket(10, I0),
    ?assertEqual(L, test_rate_pct(I1, Times, Sleeps, 10)).

%% @doc Test slow producer (inner iterator delay)
%% Rate: 20/sec (50ms per item), Inner: 30ms
%% Expected: 30, 80, 130, 180, ... (first item at inner time, then 50ms intervals)
%% The first item takes 30ms (inner) with no additional sleep
%% Runtime: ~480ms
rate_leaky_bucket_slow_producer_test() ->
    L = lists:seq(1, 10),
    Sleeps = lists:duplicate(10, 0),
    Times = [30 + N * 50 || N <- lists:seq(0, 9)],
    I0 = iterator:from_list(L),
    I1 = iterator:map(
        fun(El) ->
            timer:sleep(30),
            El
        end,
        I0
    ),
    I2 = iterator_rate:leaky_bucket(20, I1),
    ?assertEqual(L, test_rate_pct(I2, Times, Sleeps, 10)).

%% @doc Test slow consumer (longer than rate limit)
%% Rate: 20/sec (50ms), Consumer: 80ms
%% Expected: 80, 160, 240, ... (consumer dominates, first item after consumer sleep)
%% Runtime: ~720ms
rate_leaky_bucket_slow_consumer_test() ->
    L = lists:seq(1, 9),
    Sleeps = lists:duplicate(9, 80),
    Times = [N * 80 || N <- lists:seq(1, 9)],
    I0 = iterator:from_list(L),
    I1 = iterator_rate:leaky_bucket(20, I0),
    ?assertEqual(L, test_rate_pct(I1, Times, Sleeps, 10)).

%% @doc Test both slow producer and consumer
%% Rate: 20/sec (50ms), Inner: 20ms, Consumer: 20ms
%% Expected: 40, 90, 140, 190, ... (first item: 20ms consumer + 20ms inner, then 50ms intervals)
%% Runtime: ~490ms
rate_leaky_bucket_both_slow_test() ->
    L = lists:seq(1, 10),
    Sleeps = lists:duplicate(10, 20),
    Times = [40 + N * 50 || N <- lists:seq(0, 9)],
    I0 = iterator:from_list(L),
    I1 = iterator:map(
        fun(El) ->
            timer:sleep(20),
            El
        end,
        I0
    ),
    I2 = iterator_rate:leaky_bucket(20, I1),
    ?assertEqual(L, test_rate_pct(I2, Times, Sleeps, 10)).

%% @doc Test producer at rate limit (50ms inner = 20/sec rate)
%% Rate: 20/sec, Inner: 50ms
%% Expected: 50, 100, 150, 200, ... (first item at 50ms, no additional sleeping)
%% Runtime: ~500ms
rate_leaky_bucket_producer_at_limit_test() ->
    L = lists:seq(1, 10),
    Sleeps = lists:duplicate(10, 0),
    Times = [50 * (N + 1) || N <- lists:seq(0, 9)],
    I0 = iterator:from_list(L),
    I1 = iterator:map(
        fun(El) ->
            timer:sleep(50),
            El
        end,
        I0
    ),
    I2 = iterator_rate:leaky_bucket(20, I1),
    ?assertEqual(L, test_rate_pct(I2, Times, Sleeps, 10)).

%% @doc Test producer exceeds rate (10ms inner > 100/sec, but rate=20/sec)
%% Rate: 20/sec, Inner: 10ms
%% Expected: 10, 60, 110, 160, ... (first item at 10ms, then rate limiter adds 40ms to reach 50ms intervals)
%% Runtime: ~460ms
rate_leaky_bucket_producer_exceeds_limit_test() ->
    L = lists:seq(1, 10),
    Sleeps = lists:duplicate(10, 0),
    Times = [10 + N * 50 || N <- lists:seq(0, 9)],
    I0 = iterator:from_list(L),
    I1 = iterator:map(
        fun(El) ->
            timer:sleep(10),
            El
        end,
        I0
    ),
    I2 = iterator_rate:leaky_bucket(20, I1),
    ?assertEqual(L, test_rate_pct(I2, Times, Sleeps, 10)).

%% @doc Test very slow rate (1 per 2 seconds)
%% Rate: 0.5/sec (2000ms), Items: 3
%% Expected: 0, 2000, 4000 (first item immediate, then 2s intervals)
%% Runtime: ~4000ms
rate_leaky_bucket_very_slow_rate_test_() ->
    {timeout, 10, fun() ->
        L = lists:seq(1, 3),
        Sleeps = lists:duplicate(3, 0),
        Times = [N * 2000 || N <- lists:seq(0, 2)],
        I0 = iterator:from_list(L),
        I1 = iterator_rate:leaky_bucket(0.5, I0),
        ?assertEqual(L, test_rate_pct(I1, Times, Sleeps, 10))
    end}.

%% @doc Test high rate (100/sec = 10ms intervals)
%% Rate: 100/sec, Items: 10
%% Expected: 0, 10, 20, 30, ... 90ms (first item immediate, then 10ms intervals)
%% Runtime: ~90ms
rate_leaky_bucket_high_rate_test() ->
    L = lists:seq(1, 10),
    Sleeps = lists:duplicate(10, 0),
    Times = [N * 10 || N <- lists:seq(0, 9)],
    I0 = iterator:from_list(L),
    I1 = iterator_rate:leaky_bucket(100, I0),
    ?assertEqual(L, test_rate_pct(I1, Times, Sleeps, 10)).

%% @doc Test fractional rate (2.5/sec = 400ms intervals)
%% Rate: 2.5/sec, Items: 5
%% Expected: 0, 400, 800, 1200, 1600 (first item immediate, then 400ms intervals)
%% Runtime: ~1600ms
rate_leaky_bucket_fractional_rate_test() ->
    L = lists:seq(1, 5),
    Sleeps = lists:duplicate(5, 0),
    Times = [N * 400 || N <- lists:seq(0, 4)],
    I0 = iterator:from_list(L),
    I1 = iterator_rate:leaky_bucket(2.5, I0),
    ?assertEqual(L, test_rate_pct(I1, Times, Sleeps, 10)).

%% @doc Test long sequence to verify consistency
%% Rate: 50/sec (20ms), Items: 50
%% Expected: 0, 20, 40, ... 980ms (first item immediate, then 20ms intervals)
%% Runtime: ~980ms
rate_leaky_bucket_long_sequence_test() ->
    L = lists:seq(1, 50),
    Sleeps = lists:duplicate(50, 0),
    Times = [N * 20 || N <- lists:seq(0, 49)],
    I0 = iterator:from_list(L),
    I1 = iterator_rate:leaky_bucket(50, I0),
    ?assertEqual(L, test_rate_pct(I1, Times, Sleeps, 10)).

%%
%% Helper functions
%%

%% Test helper with fixed tolerance (for token_bucket tests)
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

%% Test helper with percentage-based tolerance (for leaky_bucket tests)
test_rate_pct(I, Times, Sleeps, TolerancePercent) ->
    Start = erlang:monotonic_time(millisecond),
    test_rate_pct(I, Times, Sleeps, Start, TolerancePercent).

test_rate_pct(I, [T | Times], [S | Sleeps], Start, TolerancePercent) ->
    timer:sleep(S),
    {ok, Value, I1} = iterator:next(I),
    Now = erlang:monotonic_time(millisecond),
    Elapsed = Now - Start,
    %% Use percentage tolerance with a minimum of 10ms to account for overhead
    Tolerance = max(10, round(T * TolerancePercent / 100)),
    assert_duration(T, Elapsed, Tolerance),
    [Value | test_rate_pct(I1, Times, Sleeps, Start, TolerancePercent)];
test_rate_pct(I, [], [], _, _TolerancePercent) ->
    iterator:to_list(I).

assert_duration(Expected, Duration, Tolerance) ->
    ?assert(
        abs(Expected - Duration) < Tolerance,
        #{expected => Expected, duration => Duration, tolerance => Tolerance}
    ).
