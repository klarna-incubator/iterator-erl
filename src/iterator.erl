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
    sublist/2,
    takewhile/2
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
