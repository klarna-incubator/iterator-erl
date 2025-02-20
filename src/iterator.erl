%%% @doc Abstract iterator library.
%%%
%%% It allows to define your own iterators over some continuous source of data and also build
%%% compositions, chains, filters and transformations of those iterators.
%%%
%%% Iterators are lazy, so, chains of iterators would use O(1) of memory. Also, infinite iterators
%%% are possible.
%%%
%%% It tries to mimick some of the APIs of the `lists' module where possible.
-module(iterator).

%% Primitives
-export([
    close/1,
    is_iterator/1,
    new/2,
    new/3,
    next/1
]).
%% Iterators that return another iterators (to build pipeline)
-export([
    chunks/2,
    concat/1,
    append/1,
    dropwhile/2,
    filter/2,
    filtermap/2,
    flatmap/2,
    flatten1/1,
    map/2,
    mapfoldl/3,
    nthtail/2,
    pv/3,
    report/3,
    sublist/2,
    takewhile/2,
    zip/3
]).
%% High-level iterator constructors
-export([
    eterm_fd_iterator/1,
    eterm_file_iterator/1,
    from_list/1,
    from_map/1
]).
%% Functions which consume iterators returning some value (to complete the pipeline)
-export([
    fold/3,
    foldl/3,
    foreach/2,
    search/2,
    to_list/1
]).

-export_type([iterator/1, yield_fun/1]).
-deprecated([concat/1]).

%% Iterator APIs
-type yield_fun(Type) :: fun((St :: any()) -> {Type, St :: any()} | done).
-type close_fun() :: fun((any()) -> ok).

-record(iter, {
    yield_f :: yield_fun(any()),
    state :: any(),
    close_f :: fun((State :: any()) -> ok) | undefined
}).

-opaque iterator(Type) :: #iter{yield_f :: yield_fun(Type)}.

-spec new(yield_fun(Type), InitialState) -> iterator(Type) when
    Type :: any(),
    InitialState :: any().
new(YieldFun, State) ->
    #iter{yield_f = YieldFun, state = State}.

%% @doc Creates a new iterator that needs to be explicitly "closed" when fully consumed
%% or when patially consumed but then abandoned.
%%
%% `close_fun()' should be idempotent (it could be called multiple times).
%% There is no 100% guarantee that `close_fun()' will be called! But other high-order iterators
%% from this module do their best to make sure they are closed.
-spec new(yield_fun(Type), IterState, close_fun()) -> iterator(Type) when
    Type :: any(),
    IterState :: any().
new(YieldFun, State, CloseFun) ->
    #iter{yield_f = YieldFun, state = State, close_f = CloseFun}.

close(#iter{close_f = undefined}) ->
    ok;
close(#iter{close_f = F, state = State}) ->
    ok = F(State).

%% @doc Returns next element of the iterator and new iterator, or `done' when iterator is empty
%%
%% XXX: If you don't plan to consume the iterator to the end, make sure you call `close/1' on it
%% before abandoning.
-spec next(iterator(Type)) -> {ok, Type, iterator(Type)} | done when
    Type :: any().
next(#iter{yield_f = Fun, state = State} = Iter) ->
    case Fun(State) of
        done ->
            close(Iter),
            done;
        {Data, NewState} ->
            {ok, Data, Iter#iter{state = NewState}}
    end.

%% @doc Returns `true' if argument provided is `iterator()'
-spec is_iterator(any()) -> boolean().
is_iterator(#iter{}) -> true;
is_iterator(_) -> false.

%% @doc Converts the iterator to the list
%%
%% XXX: Might run OOM if called over the infinite iterator!
-spec to_list(iterator(Type)) -> [Type] when
    Type :: any().
to_list(Iter) ->
    case next(Iter) of
        {ok, Data, NewIter} ->
            [Data | to_list(NewIter)];
        done ->
            []
    end.

%% @doc Converts list to iterator that yields the elements of this list
-spec from_list(list(Type)) -> iterator(Type) when
    Type :: any().
from_list(List) ->
    new(fun yield_from_list/1, List).

yield_from_list([E | Tail]) ->
    {E, Tail};
yield_from_list([]) ->
    done.

%% @doc Converts map to the iterator that yields `{Key, Value}' tuples in unspecified order
%%
%% Similar to `maps:to_list/1'
-spec from_map(#{Key => Val}) -> iterator({Key, Val}).
from_map(Map) ->
    MapsIter = maps:iterator(Map),
    new(fun yield_from_map/1, MapsIter).

yield_from_map(MapsIter) ->
    case maps:next(MapsIter) of
        {Key, Value, NextIter} ->
            {{Key, Value}, NextIter};
        none ->
            done
    end.

%% @doc Returns the first element that matches the predicate, otherwise `false'
-spec search(fun((Type) -> boolean()), iterator(Type)) -> {value, Type} | false when
    Type :: any().
search(Fun, Iterator) ->
    case next(Iterator) of
        {ok, Data, NewIter} ->
            case Fun(Data) of
                true ->
                    close(NewIter),
                    {value, Data};
                false ->
                    search(Fun, NewIter)
            end;
        done ->
            false
    end.

%% @doc Folds over the iterator, returning the fold accumulator
%% Similar to `lists:foldl/2'
-spec fold(fun((IterType, Acc) -> Acc), Acc, iterator:iterator(IterType)) -> Acc when
    Acc :: any(),
    IterType :: any().
fold(Fun, Acc, Iterator) ->
    case next(Iterator) of
        done ->
            Acc;
        {ok, Data, NewIterator} ->
            fold(Fun, Fun(Data, Acc), NewIterator)
    end.

%% @doc Convenient alias for `fold/3'
foldl(Fun, Acc, Iterator) ->
    fold(Fun, Acc, Iterator).

%% @doc Applies `Fun' to each element of the iterator, discarding the results
%% It does not return a new iterator, but atom ok and always consumes the iterator to the end.
%% Only makes sense for side-effecting functions.
%% Similar to `lists:foreach/2'
-spec foreach(fun((Type) -> any()), iterator(Type)) -> ok when
    Type :: any().
foreach(Fun, Iterator) ->
    case next(Iterator) of
        {ok, Data, NewIterator} ->
            Fun(Data),
            foreach(Fun, NewIterator);
        done ->
            ok
    end.

%% @doc Returns a new iterator that yields results of application of the `Fun' to the each element
%% of `InnerIterator'.
%% Similar to `lists:map/2'
-spec map(fun((InType) -> OutType), iterator(InType)) -> iterator(OutType) when
    InType :: any(),
    OutType :: any().
map(Fun, InnerIterator) ->
    new(fun yield_map/1, {Fun, InnerIterator}).

yield_map({Fun, InnerIter}) ->
    case next(InnerIter) of
        {ok, Data, NewInnerIter} ->
            {Fun(Data), {Fun, NewInnerIter}};
        done ->
            done
    end.

%% @doc Stateful `map'
%%
%% Somewhat equivalent of `lists:mapfoldl/2'---transforms each element
%% of the sequence while keeping a stateful context.
%%
%% It does not emit intermediate or final states, like `lists:mapfoldl/2' does,
%% for simplicity.
-spec mapfoldl(fun((A, AccIn) -> {B, AccOut}), AccIn, iterator(A)) -> iterator(B) when
    A :: term(),
    B :: term(),
    AccIn :: term(),
    AccOut :: term().
mapfoldl(Fun, InitialState, InnerIterator) ->
    new(fun yield_mapfoldl/1, {Fun, InitialState, InnerIterator}).

yield_mapfoldl({Fun, State0, InnerIterator0}) ->
    case next(InnerIterator0) of
        {ok, Data0, InnerIterator} ->
            {Data, State} = Fun(Data0, State0),
            {Data, {Fun, State, InnerIterator}};
        done ->
            done
    end.

%% @doc Discards first N elements of inner iterator
%% If inner iterator procuced less than N elements, it fails with `badarg' error.
%%
%% Similar to `lists:nthtail/2'
-spec nthtail(non_neg_integer(), iterator(Data)) -> iterator(Data) when
    Data :: any().
nthtail(N, InnerIterator) ->
    new(fun yield_nthtail/1, {N, InnerIterator}).

yield_nthtail({0, InnerIter}) ->
    case next(InnerIter) of
        {ok, Data, NewInnerIter} ->
            {Data, {0, NewInnerIter}};
        done ->
            done
    end;
yield_nthtail({N, InnerIter}) ->
    case next(InnerIter) of
        {ok, _Data, NewInnerIter} ->
            yield_nthtail({N - 1, NewInnerIter});
        done ->
            error(too_short)
    end.

%% @doc Consumes the iterator that returns a list item and yields list elements one-by-one
%% Opposite of `chunks/1'
-spec flatmap(fun((InType) -> [OutType]), iterator(InType)) -> iterator(OutType) when
    InType :: any(),
    OutType :: any().
flatmap(Fun, InnerIterator) ->
    new(fun yield_flatmap/1, {Fun, [], InnerIterator}).

yield_flatmap({Fun, [], InnerIter}) ->
    case next(InnerIter) of
        {ok, Element, NewInnerIter} ->
            yield_flatmap({Fun, Fun(Element), NewInnerIter});
        done ->
            done
    end;
yield_flatmap({Fun, [Item | Tail], InnerIter}) ->
    {Item, {Fun, Tail, InnerIter}}.

%% @doc Converts iterator that yields lists to flat list (non-recursive)
%%
%% Do not confuse with `append/1'. This function takes SINGLE iterator that yields
%% lists and returns list elements one-by-one.
-spec flatten1(iterator([Type])) -> iterator(Type).
flatten1(InnerIterator) ->
    flatmap(fun(V) -> V end, InnerIterator).

%% @doc Returns a new iterator that yields elements of `InnerIterator' which were allowed by `Fun'
%% Similar to `lists:filter/2'
-spec filter(fun((Type) -> boolean()), iterator(Type)) -> iterator(Type) when
    Type :: any().
filter(Fun, InnerIterator) ->
    new(fun yield_filter/1, {Fun, InnerIterator}).

yield_filter({Fun, InnerIter}) ->
    case next(InnerIter) of
        {ok, Data, NewInnerIter} ->
            case Fun(Data) of
                true ->
                    {Data, {Fun, NewInnerIter}};
                false ->
                    yield_filter({Fun, NewInnerIter})
            end;
        done ->
            done
    end.

%% @doc Returns a new iterator that yields (optionally modified) elements of `InnerIterator'
%% if `Fun' returns `true' or `{true, NewValue}' and skips the element if `Fun' returns `false'.
%%
%% Similar to `lists:filtermap/2'
filtermap(Fun, InnerIterator) ->
    new(fun yield_filtermap/1, {Fun, InnerIterator}).

yield_filtermap({Fun, InnerIter}) ->
    case next(InnerIter) of
        {ok, Data, NewInnerIter} ->
            case Fun(Data) of
                true ->
                    {Data, {Fun, NewInnerIter}};
                {true, NewData} ->
                    {NewData, {Fun, NewInnerIter}};
                false ->
                    yield_filtermap({Fun, NewInnerIter})
            end;
        done ->
            done
    end.

%% @doc Joins multiple iterators to a single steam (non-recursive, only top-level)
%% Similar to `lists:append/1'
-spec append([iterator(any())]) -> iterator(any()).
append(InnerIterators) ->
    new(fun yield_append/1, InnerIterators).

yield_append([CurrentIter | Tail]) ->
    case next(CurrentIter) of
        {ok, Item, NewCurrentIter} ->
            {Item, [NewCurrentIter | Tail]};
        done ->
            yield_append(Tail)
    end;
yield_append([]) ->
    done.

%% @doc deprecated
concat(Inner) ->
    append(Inner).

%% @doc Skips elements of inner iterator until `Fun' returns `true'
%% Similar to `lists:dropwhile/2'
-spec dropwhile(fun((Type) -> boolean()), iterator(Type)) -> iterator(Type) when
    Type :: any().
dropwhile(Fun, InnerIter) ->
    new(fun yield_dropwhile/1, {dropwhile, Fun, InnerIter}).

yield_dropwhile({dropwhile, Fun, InnerIter}) ->
    case next(InnerIter) of
        {ok, Data, NewInnerIter} ->
            case Fun(Data) of
                true ->
                    yield_dropwhile({dropwhile, Fun, NewInnerIter});
                false ->
                    {Data, NewInnerIter}
            end;
        done ->
            done
    end;
yield_dropwhile(InnerIter) ->
    case next(InnerIter) of
        {ok, Data, NewInnerIter} ->
            {Data, NewInnerIter};
        done ->
            done
    end.

%% @doc Yields elements of inner iterator as long as `Fun' returns `true'
%% Similar to `lists:takewhile/2'
-spec takewhile(fun((Type) -> boolean()), iterator(Type)) -> iterator(Type) when
    Type :: any().
takewhile(Fun, InnerIter) ->
    new(fun yield_takewhile/1, {Fun, InnerIter}).

yield_takewhile({Fun, InnerIter}) ->
    case next(InnerIter) of
        {ok, Data, NewInnerIter} ->
            case Fun(Data) of
                true ->
                    {Data, {Fun, NewInnerIter}};
                false ->
                    % since we did not consume the whole InnerIter
                    close(NewInnerIter),
                    done
            end;
        done ->
            done
    end.

%% @doc Yields at most `N' elements of the inner iterator
%% Similar to `lists:sublist/2'
-spec sublist(iterator(Type), non_neg_integer()) -> iterator(Type) when
    Type :: any().
sublist(InnerIter, N) when N >= 0 ->
    new(fun yield_n/1, {N, InnerIter}).

yield_n({0, InnerIter}) ->
    % since we did not consume the whole InnerIter
    close(InnerIter),
    done;
yield_n({N, InnerIter}) ->
    case next(InnerIter) of
        {ok, Item, NewInnerIter} ->
            {Item, {N - 1, NewInnerIter}};
        done ->
            done
    end.

%% @doc Takes up to N items from sub-iterator at a time and yields them as non-empty list
%% Last chunk may contain less than `N' elements.
%% Kind of similar to `lists:split/2' but repeated recursively and not throwing errors.
-spec chunks(iterator(Data), pos_integer()) -> iterator([Data, ...]) when
    Data :: any().
chunks(InnerIterator, N) ->
    new(fun yield_chunk/1, {InnerIterator, N}).

yield_chunk({InnerIterator, N}) ->
    case consume_n(next(InnerIterator), N - 1, []) of
        {Chunk, NewInnerIterator} ->
            {Chunk, {NewInnerIterator, N}};
        [] ->
            done;
        Chunk when is_list(Chunk) ->
            {Chunk, '$done'}
    end;
yield_chunk('$done') ->
    done.

consume_n({ok, Data, InnerIterator}, 0, Acc) ->
    {lists:reverse([Data | Acc]), InnerIterator};
consume_n({ok, Data, InnerIterator}, N, Acc) ->
    consume_n(next(InnerIterator), N - 1, [Data | Acc]);
consume_n(done, _, Acc) ->
    lists:reverse(Acc).

%% @doc Consumes 2 iterators at the same time, returns an iterator that yields
%% 2-tuples containing next element of each iterator.
%% If one of the iterators is done before the other, the behaviour would depend
%% on `How' parameter:
%% * If it is `trim', then the other iterator will be closed and `zip/3' would finish.
%% * If it is `{pad, {Default1, Default2}', then the values of the finished iterator
%%   would be replaced with Default until the 2nd iterator is done.
%%
%% Similar to OTP-26+ `lists:zip/3', but doesn't support `fail' behaviour (just because
%% it rarely makes sense for lazy sequences; feel free to file an issue otherwise).
-spec zip(iterator(Data1), iterator(Data2), trim | {pad, {Default1, Default2}}) ->
    iterator({Data1 | Default1, Data2 | Default2})
when
    Data1 :: any(),
    Data2 :: any(),
    Default1 :: any(),
    Default2 :: any().
zip(Iter1, Iter2, How) ->
    new(fun yield_zip/1, {Iter1, Iter2, How}).

yield_zip({Iter1, Iter2, How}) ->
    case {maybe_next(Iter1), maybe_next(Iter2)} of
        {{ok, Item1, NewIter1}, {ok, Item2, NewIter2}} ->
            {{Item1, Item2}, {NewIter1, NewIter2, How}};
        {done, done} ->
            done;
        {done, {ok, _Item2, NewIter2}} when How =:= trim ->
            close(NewIter2),
            done;
        {{ok, _Item1, NewIter1}, done} when How =:= trim ->
            close(NewIter1),
            done;
        {done, {ok, Item2, NewIter2}} when element(1, How) =:= pad ->
            {pad, {Default1, _}} = How,
            {{Default1, Item2}, {done, NewIter2, How}};
        {{ok, Item1, NewIter1}, done} when element(1, How) =:= pad ->
            {pad, {_, Default2}} = How,
            {{Item1, Default2}, {NewIter1, done, How}}
    end.

maybe_next(done) ->
    done;
maybe_next(#iter{} = Iter) ->
    next(Iter).

-record(pv, {
    f :: fun((any(), integer(), integer(), integer()) -> any()),
    for_each_n :: pos_integer(),
    every_s :: pos_integer(),
    last_report_time :: integer(),
    last_report_n :: non_neg_integer(),
    total_n :: non_neg_integer(),
    inner_i :: iterator:iterator(any())
}).

%% @doc Passthrough iterator one can use to periodically report the progress of the inner iterator.
%% Name comes from `pv' (pipe view) Unix utility. See `man pv'.
%% @param F function to call when one of the conditions triggers. Function arguments:
%%   - `Data' - current element of the inner iterator (sample)
%%   - `TimePassed' - time passed since the last report in native units
%%   - `ItemsPassed' - number of items passed since the last report
%%   - `TotalItems' - total number of items passed since the start
%% `TimePassed' + `ItemsPassed' are convenient to calculate the speed of the stream.
%% @param Opts trigger condition options:
%%   - `for_each_n' - trigger every N-th element
%%   - `every_s' - trigger every S seconds
%% @param InnerIter inner iterator to wrap
%%
%% Keep in mind that whichever trigger condition is met first, the `F' function will be called and
%% counters/timers will reset. So if you set `for_each_n' to 1000 and `every_s' to 30, then the
%% function will be called either as counter reaches 1000 or 30 seconds pass since the last call.
%%
%% If it takes more than `every_s' seconds to process a single element, the function will be called
%% with additional delay.
-spec pv(
    fun(
        (Type, TimePassed :: integer(), ItemsPassed :: integer(), TotalItems :: integer()) -> any()
    ),
    #{
        for_each_n => pos_integer(),
        every_s => pos_integer()
    },
    iterator:iterator(Type)
) -> iterator:iterator(Type) when
    Type :: any().
pv(F, Opts, InnerIter) when is_function(F, 4) ->
    Start = erlang:monotonic_time(),
    State = #pv{
        f = F,
        every_s = maps:get(every_s, Opts, 30),
        for_each_n = maps:get(for_each_n, Opts, 1000),
        last_report_time = Start,
        last_report_n = 0,
        total_n = 0,
        inner_i = InnerIter
    },
    iterator:new(fun yield_pv/1, State).

yield_pv(
    #pv{
        f = F,
        every_s = TimeTrigger,
        for_each_n = CountTrigger,
        last_report_time = LastReportT,
        last_report_n = LastReportN,
        total_n = N,
        inner_i = InnerIter
    } = St
) ->
    case iterator:next(InnerIter) of
        {ok, Data, NewInnerIter} ->
            NextN = N + 1,
            ItemsProcessed = NextN - LastReportN,
            CountCondition = ItemsProcessed >= CountTrigger,
            Now = erlang:monotonic_time(),
            TimePassed = Now - LastReportT,
            TimeCondition = erlang:convert_time_unit(TimePassed, native, second) >= TimeTrigger,
            if
                CountCondition orelse TimeCondition ->
                    F(Data, TimePassed, ItemsProcessed, NextN),
                    {Data, St#pv{
                        last_report_time = Now,
                        last_report_n = NextN,
                        total_n = NextN,
                        inner_i = NewInnerIter
                    }};
                true ->
                    {Data, St#pv{
                        total_n = NextN,
                        inner_i = NewInnerIter
                    }}
            end;
        done ->
            done
    end.

%% @doc Alias for `pv/3'
report(F, Opts, InnerIter) ->
    pv(F, Opts, InnerIter).

%% @doc Iterator over .eterm file (file containing dot-terminated Erlang terms)
%% XXX: never abandon this iterator from long-running processes! It would leak file descriptor!
%% Either consume it to the end or close with `iterator:close/1' explicitly.
-spec eterm_file_iterator(file:name_all()) -> iterator(any()).
eterm_file_iterator(Filename) ->
    {ok, Fd} = file:open(Filename, [read, read_ahead]),
    iterator:new(fun yield_eterm/1, Fd, fun close_eterm/1).

%% @doc Iterator over already opened .eterm file
%% It's up to the caller to make sure file is opened with correct options and closed when done.
-spec eterm_fd_iterator(file:io_device()) -> iterator(any()).
eterm_fd_iterator(Fd) ->
    iterator:new(fun yield_eterm/1, Fd).

yield_eterm(Fd) ->
    case io:read(Fd, '') of
        {ok, Term} ->
            {Term, Fd};
        eof ->
            done;
        {error, Reason} ->
            error(Reason)
    end.

close_eterm(Fd) ->
    file:close(Fd).
