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
