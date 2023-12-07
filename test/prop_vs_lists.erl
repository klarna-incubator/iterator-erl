%% @doc Proper tests for `iterator' module
-module(prop_vs_lists).

%% Tests
-export([
    prop_from_to_list/0,
    prop_from_map_to_list/0,
    prop_fold/0,
    prop_map/0,
    prop_mapfoldl/0,
    prop_flatmap/0,
    prop_filter/0,
    prop_dropwhile/0,
    prop_takewhile/0,
    prop_search/0,
    prop_sublist/0,
    prop_chunks/0,
    prop_chunk_flatten/0,
    prop_concat/0,
    prop_combination/0
]).

-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").

prop_from_to_list() ->
    Gen = proper_types:list(),
    ?FORALL(
        List,
        Gen,
        begin
            ListIter = iterator:from_list(List),
            ?assertEqual(List, iterator:to_list(ListIter)),
            true
        end
    ).

prop_from_map_to_list() ->
    Gen = proper_types:map(),
    ?FORALL(
        Map,
        Gen,
        begin
            MapIter = iterator:from_map(Map),
            ?assertEqual(
                lists:sort(maps:to_list(Map)),
                lists:sort(iterator:to_list(MapIter))
            ),
            true
        end
    ).

prop_fold() ->
    Gen = proper_types:list(proper_types:integer()),
    ?FORALL(
        ListOfInts,
        Gen,
        begin
            ListIter = iterator:from_list(ListOfInts),
            F = fun erlang:'+'/2,
            ?assertEqual(
                lists:foldl(F, 0, ListOfInts),
                iterator:fold(F, 0, ListIter)
            ),
            true
        end
    ).

prop_map() ->
    Gen = proper_types:list(proper_types:integer()),
    ?FORALL(
        List,
        Gen,
        begin
            ListIter = iterator:from_list(List),
            F = fun(V) -> V + 1 end,
            ?assertEqual(
                lists:map(F, List),
                iterator:to_list(iterator:map(F, ListIter))
            ),
            true
        end
    ).

prop_mapfoldl() ->
    Gen = proper_types:list(proper_types:integer()),
    ?FORALL(
        List,
        Gen,
        begin
            ListIter = iterator:from_list(List),
            F = fun(A, B) -> {A + B, A + B} end,
            {Result, _} = lists:mapfoldl(F, 0, List),
            ?assertEqual(Result, iterator:to_list(iterator:mapfoldl(F, 0, ListIter))),
            true
        end
    ).

prop_flatmap() ->
    Gen = proper_types:list(proper_types:list()),
    ?FORALL(
        List,
        Gen,
        begin
            ListIter = iterator:from_list(List),
            F = fun(V) -> V end,
            ?assertEqual(
                lists:flatmap(F, List),
                iterator:to_list(iterator:flatmap(F, ListIter))
            ),
            true
        end
    ).

prop_filter() ->
    Gen = proper_types:list(proper_types:integer()),
    ?FORALL(
        List,
        Gen,
        begin
            ListIter = iterator:from_list(List),
            F = fun(V) -> V > 0 end,
            ?assertEqual(
                lists:filter(F, List),
                iterator:to_list(iterator:filter(F, ListIter))
            ),
            true
        end
    ).

prop_dropwhile() ->
    Gen = proper_types:list(proper_types:integer()),
    ?FORALL(
        List,
        Gen,
        begin
            ListIter = iterator:from_list(List),
            F = fun(V) -> V < 5 end,
            ?assertEqual(
                lists:dropwhile(F, List),
                iterator:to_list(iterator:dropwhile(F, ListIter))
            ),
            true
        end
    ).

prop_takewhile() ->
    Gen = proper_types:list(proper_types:integer()),
    P = ?FORALL(
        List,
        Gen,
        begin
            ListIter = iterator:from_list(List),
            F = fun(V) -> V < 5 end,
            ?assertEqual(
                lists:takewhile(F, List),
                iterator:to_list(iterator:takewhile(F, ListIter))
            ),
            true
        end
    ).

prop_search() ->
    Gen = {proper_types:integer(), proper_types:list(proper_types:integer())},
    P = ?FORALL(
        {Needle, Haystack},
        Gen,
        begin
            HayIter = iterator:from_list(Haystack),
            Pred = fun(V) -> V =:= Needle end,
            ?assertEqual(
                lists:search(Pred, Haystack),
                iterator:search(Pred, HayIter),
                [
                    {needle, Needle},
                    {haystack, Haystack}
                ]
            ),
            true
        end
    ).

prop_sublist() ->
    Gen = {proper_types:pos_integer(), proper_types:list()},
    ?FORALL(
        {Len, List},
        Gen,
        begin
            ListIter = iterator:from_list(List),
            ?assertEqual(
                lists:sublist(List, Len),
                iterator:to_list(iterator:sublist(ListIter, Len))
            ),
            ?assertEqual(
                lists:sublist(List, Len div 2),
                iterator:to_list(iterator:sublist(ListIter, Len div 2))
            ),
            true
        end
    ).

prop_chunks() ->
    Gen = {proper_types:integer(1, 10), proper_types:list(proper_types:integer())},
    ?FORALL(
        {ChunkSize, List},
        Gen,
        begin
            ListIter = iterator:from_list(List),
            ChunkedList = iterator:to_list(iterator:chunks(ListIter, ChunkSize)),
            %% none of the chunks should be longer than `ChunkSize'
            lists:foreach(
                fun(Chunk) ->
                    ?assert(length(Chunk) =< ChunkSize)
                end,
                ChunkedList
            ),
            %% concatenated chunks are equal to original list
            ?assertEqual(
                List,
                lists:append(ChunkedList),
                [
                    {list, List},
                    {chunk_size, ChunkSize}
                ]
            ),
            %% Should never yield a chunk with empty list
            ?assertNot(
                lists:any(
                    fun
                        ([]) -> true;
                        (_) -> false
                    end,
                    ChunkedList
                )
            ),
            true
        end
    ).

%% @doc Tests that `flatmap' with ID function would revert `chunks'
prop_chunk_flatten() ->
    Gen = {proper_types:integer(1, 10), proper_types:list(proper_types:integer())},
    ?FORALL(
        {ChunkSize, List},
        Gen,
        begin
            ListIter = iterator:from_list(List),
            ChunkIter = iterator:chunks(ListIter, ChunkSize),
            FlatIter = iterator:flatmap(fun(E) -> E end, ChunkIter),
            ?assertEqual(
                List,
                iterator:to_list(FlatIter),
                [{chunk_size, ChunkSize}]
            ),
            true
        end
    ).

prop_concat() ->
    Gen = proper_types:list(proper_types:list()),
    ?FORALL(
        ListOfLists,
        Gen,
        begin
            Iterators = lists:map(fun iterator:from_list/1, ListOfLists),
            ?assertEqual(
                lists:concat(ListOfLists),
                iterator:to_list(iterator:concat(Iterators))
            ),
            true
        end
    ).

%% @doc Tests that various high-order iterators can be chained together
prop_combination() ->
    Gen = proper_types:list(proper_types:integer()),
    ?FORALL(
        UnsortedList,
        Gen,
        begin
            List0 = lists:sort(UnsortedList),
            Iter0 = iterator:from_list(List0),

            DropFun = fun(V) -> V < -5 end,
            TakeFun = fun(V) -> V < 20 end,
            FilterFun = fun(V) -> V rem 2 =:= 0 end,
            MapFun = fun(V) -> {V} end,

            Iter1 = iterator:dropwhile(DropFun, Iter0),
            Iter2 = iterator:takewhile(TakeFun, Iter1),
            Iter3 = iterator:filter(FilterFun, Iter2),
            Iter = iterator:map(MapFun, Iter3),

            List1 = lists:dropwhile(DropFun, List0),
            List2 = lists:takewhile(TakeFun, List1),
            List3 = lists:filter(FilterFun, List2),
            List = lists:map(MapFun, List3),
            ?assertEqual(
                List,
                iterator:to_list(Iter),
                [{list0, List0}]
            ),
            true
        end
    ).
