%%% @doc Passthrough iterators for rate limiting.
-module(iterator_rate).

-export([token_bucket/2]).

% Token bucket record to represent the state
-record(token_bucket, {
    %% Constants

    % Tokens added per rate window
    rate,
    % Maximum number of tokens in the bucket
    capacity,
    % Rate window duration in milliseconds
    rate_window_ms,
    %% Variables

    % Current number of tokens in the bucket
    tokens,
    % Last time the tokens were updated
    last_refill,
    % Inner iterator
    inner_iterator
}).

%% @doc Token bucket shaper
%% It tries to yield not more than "rate" items from inner iterator per "rate_window_ms"
%% milliseconds window. It uses `timer:sleep/1' to slow down if necessary.
%% However it can release bursts of items up to "capacity".
%% This implementation assumes each item of the inner iterator consumes exactly one token.
%% @param Opts Rate limiter configuration:
%%   rate: how many "tokens" to add per time-window (default: 1)
%%   window_ms: the size of the time window in milliseconds (default: 1000)
%%   capacity: how many tokens can be accumulated for bursts (default: 1)
token_bucket(Opts, InnerI) ->
    #{
        rate := Rate,
        capacity := Capacity,
        window_ms := RateWindowMs
    } = maps:merge(
        #{
            rate => 1,
            capacity => 1,
            window_ms => 1000
        },
        Opts
    ),
    Rate > 0 orelse error(invalid_rate),
    Capacity > 0 orelse error(invalid_capacity),
    RateWindowMs > 0 orelse error(invalid_window_ms),
    iterator:new(
        fun token_bucket_yield/1,
        #token_bucket{
            rate = Rate,
            capacity = Capacity,
            rate_window_ms = RateWindowMs,
            tokens = Capacity,
            last_refill = erlang:monotonic_time(millisecond),
            inner_iterator = InnerI
        }
    ).

token_bucket_yield(
    #token_bucket{
        rate = Rate,
        capacity = Capacity,
        rate_window_ms = RateWindowMs,
        tokens = Tokens,
        last_refill = LastRefill,
        inner_iterator = InnerI
    } = Bucket
) ->
    %% refill (todo: refill lazily)
    Now = erlang:monotonic_time(millisecond),
    TimePassedMs = Now - LastRefill,
    AddedTokens = round(TimePassedMs * Rate / RateWindowMs),
    NewTokens = min(Capacity, Tokens + AddedTokens),
    UpdatedBucket = Bucket#token_bucket{tokens = NewTokens, last_refill = Now},
    %% consume
    case NewTokens > 0 of
        true ->
            case iterator:next(InnerI) of
                {ok, Data, NewInnerI} ->
                    {Data, UpdatedBucket#token_bucket{
                        tokens = NewTokens - 1, inner_iterator = NewInnerI
                    }};
                done ->
                    done
            end;
        false ->
            %% Sleep just enough to get at least one token
            TimeToWaitMs = RateWindowMs / Rate,
            timer:sleep(ceil(TimeToWaitMs)),
            token_bucket_yield(UpdatedBucket)
    end.
