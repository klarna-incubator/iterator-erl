%% @doc Property-based tests for pmap
-module(prop_pmap).

%% Tests
-export([
    prop_order_is_preserved/0,
    prop_vs_lists/0,
    prop_interrupted/0,
    prop_chained/0
]).

-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").

%% @doc Test that the order of the input list is preserved
prop_order_is_preserved() ->
    Gen = {
        proper_types:list(),
        proper_types:oneof([
            infinity,
            proper_types:integer(1000, 10000)
        ]),
        proper_types:pos_integer()
    },
    ?FORALL(
        {List, T, C},
        Gen,
        begin
            ListIter = iterator:from_list(List),
            Links0 = links(),
            PmapIter =
                iterator_pmap:pmap(
                    fun(X) -> X end,
                    ListIter,
                    #{
                        recv_timeout => T,
                        concurrency => C
                    }
                ),
            ?assertEqual(List, iterator:to_list(PmapIter)),
            ?assertEqual([], iterator_pmap:flush()),
            assert_links(Links0),
            true
        end
    ).

%% @doc The outcome of pmap is the same as of `lists:map/2'
prop_vs_lists() ->
    Gen = {
        proper_types:list(proper_types:integer()),
        proper_types:oneof([
            infinity,
            proper_types:integer(1000, 10000)
        ]),
        proper_types:pos_integer()
    },
    ?FORALL(
        {List, T, C},
        Gen,
        begin
            F = fun(X) -> X + 1 end,
            ListIter = iterator:from_list(List),
            Links0 = links(),
            PmapIter =
                iterator_pmap:pmap(
                    F,
                    ListIter,
                    #{
                        recv_timeout => T,
                        concurrency => C
                    }
                ),
            ?assertEqual(
                lists:map(F, List),
                iterator:to_list(PmapIter)
            ),
            ?assertEqual([], iterator_pmap:flush()),
            assert_links(Links0),
            true
        end
    ).

%% @doc Make sure workers are cleaned if pmap iterator is not fully consumed
prop_interrupted() ->
    Gen0 = {
        proper_types:non_empty(proper_types:list(proper_types:integer())),
        proper_types:pos_integer()
    },
    Gen = ?LET(
        {List, C},
        Gen0,
        {
            List,
            C,
            proper_types:integer(1, length(List))
        }
    ),
    ?FORALL(
        {List, C, TakeN},
        Gen,
        begin
            F = fun(X) -> X + 1 end,
            ListIter = iterator:from_list(List),
            Links0 = links(),
            PmapIter =
                iterator_pmap:pmap(
                    F,
                    ListIter,
                    #{concurrency => C}
                ),
            FirstNIter = iterator:sublist(PmapIter, TakeN),
            ?assertEqual(
                lists:sublist(lists:map(F, List), TakeN),
                iterator:to_list(FirstNIter)
            ),
            ?assertEqual([], iterator_pmap:flush()),
            assert_links(Links0),
            true
        end
    ).

%% @doc Make sure several pmap's can be chained.
prop_chained() ->
    Gen = {
        proper_types:list(proper_types:integer()),
        proper_types:pos_integer(),
        proper_types:pos_integer()
    },
    ?FORALL(
        {List, C1, C2},
        Gen,
        begin
            F1 = fun(X) -> X + 1 end,
            F2 = fun(X) -> X * 2 end,
            ListIter = iterator:from_list(List),
            Links0 = links(),
            Pmap1Iter =
                iterator_pmap:pmap(
                    F1,
                    ListIter,
                    #{concurrency => C1}
                ),
            Pmap2Iter =
                iterator_pmap:pmap(
                    F2,
                    Pmap1Iter,
                    #{concurrency => C2}
                ),
            ?assertEqual(
                lists:map(F2, lists:map(F1, List)),
                iterator:to_list(Pmap2Iter)
            ),
            ?assertEqual([], iterator_pmap:flush()),
            assert_links(Links0),
            true
        end
    ).

links() ->
    {links, L} = process_info(self(), links),
    lists:sort(L).

assert_links(Links0) ->
    Links = links(),
    ?assertEqual(
        Links0,
        Links,
        [
            {extra, [
                {P, erlang:process_info(P)}
             || P <- ordsets:subtract(Links, Links0)
            ]}
        ]
    ).
