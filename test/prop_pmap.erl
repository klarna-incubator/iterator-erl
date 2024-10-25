%% @doc Property-based tests for pmap
-module(prop_pmap).

%% Tests
-export([
    prop_order_is_preserved/0,
    prop_unordered/0,
    prop_vs_lists/0,
    prop_interrupted/0,
    prop_chained/0,
    prop_timeout/0
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
            Tag = erlang:make_ref(),
            PmapIter =
                iterator_pmap:pmap(
                    fun(X) -> X end,
                    ListIter,
                    #{
                        recv_timeout => T,
                        concurrency => C,
                        tag => Tag
                    }
                ),
            ?assertEqual(List, iterator:to_list(PmapIter)),
            ?assertEqual([], iterator_pmap:flush(Tag)),
            assert_links(Links0),
            true
        end
    ).

%% @doc Test that unordered iterator processes all the elements
prop_unordered() ->
    Gen = {
        proper_types:list({proper_types:integer(0, 30), proper_types:term()}),
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
            Tag = erlang:make_ref(),
            PmapIter =
                iterator_pmap:pmap(
                    fun({Sleep, _} = El) ->
                        timer:sleep(Sleep),
                        El
                    end,
                    ListIter,
                    #{
                        recv_timeout => T,
                        concurrency => C,
                        tag => Tag,
                        ordered => false
                    }
                ),
            %% We can't check that the order is different, but we can check that all elements
            %% are processed
            ?assertEqual(lists:sort(List), lists:sort(iterator:to_list(PmapIter))),
            ?assertEqual([], iterator_pmap:flush(Tag)),
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
            Tag = erlang:make_ref(),
            PmapIter =
                iterator_pmap:pmap(
                    F,
                    ListIter,
                    #{
                        recv_timeout => T,
                        concurrency => C,
                        tag => Tag
                    }
                ),
            ?assertEqual(
                lists:map(F, List),
                iterator:to_list(PmapIter)
            ),
            ?assertEqual([], iterator_pmap:flush(Tag)),
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
            Tag = erlang:make_ref(),
            PmapIter =
                iterator_pmap:pmap(
                    F,
                    ListIter,
                    #{concurrency => C, tag => Tag}
                ),
            FirstNIter = iterator:sublist(PmapIter, TakeN),
            ?assertEqual(
                lists:sublist(lists:map(F, List), TakeN),
                iterator:to_list(FirstNIter)
            ),
            ?assertEqual([], iterator_pmap:flush(Tag)),
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
            Tag1 = erlang:make_ref(),
            Tag2 = erlang:make_ref(),
            Pmap1Iter =
                iterator_pmap:pmap(
                    F1,
                    ListIter,
                    #{concurrency => C1, tag => Tag1}
                ),
            Pmap2Iter =
                iterator_pmap:pmap(
                    F2,
                    Pmap1Iter,
                    #{concurrency => C2, tag => Tag2}
                ),
            ?assertEqual(
                lists:map(F2, lists:map(F1, List)),
                iterator:to_list(Pmap2Iter)
            ),
            ?assertEqual([], iterator_pmap:flush(Tag1)),
            ?assertEqual([], iterator_pmap:flush(Tag2)),
            assert_links(Links0),
            true
        end
    ).

%% @doc Result receive timeout shuts down the pool and generates a `timeout' exception
prop_timeout() ->
    Gen = {
        proper_types:non_empty(proper_types:list(proper_types:pos_integer())),
        proper_types:pos_integer()
    },
    ?FORALL(
        {List, C},
        Gen,
        begin
            F = fun(X) -> timer:sleep(min(1000, X * 10)) end,
            ListIter = iterator:from_list(List),
            Links0 = links(),
            Tag = erlang:make_ref(),
            PmapIter =
                iterator_pmap:pmap(
                    F,
                    ListIter,
                    #{
                        concurrency => C,
                        recv_timeout => 0,
                        tag => Tag
                    }
                ),
            ?assertError(
                timeout,
                iterator:to_list(PmapIter)
            ),
            ?assertEqual([], iterator_pmap:flush(Tag)),
            assert_links(Links0),
            true
        end
    ).

links() ->
    {links, L} = process_info(self(), links),
    lists:sort(L).

assert_links(Links0) ->
    %% Wait up to 50 * 100 = 5s
    assert_links(Links0, 50, 100).

assert_links(Links0, N, Sleep) when N > 0 ->
    Links = links(),
    if
        length(Links0) =:= length(Links) ->
            ?assertEqual(
                Links0,
                Links,
                [
                    {extra, [
                        {P, erlang:process_info(P)}
                     || P <- ordsets:subtract(Links, Links0)
                    ]}
                ]
            );
        true ->
            timer:sleep(Sleep),
            assert_links(Links0, N - 1, Sleep)
    end;
assert_links(Links, _, _) ->
    ?assertEqual(Links, links()).
