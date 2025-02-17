# iterator.erl
> Lazy sequences simulating stdlib `lists` module API

[![CI checks](https://github.com/klarna-incubator/iterator-erl/actions/workflows/ci.yml/badge.svg)](https://github.com/klarna-incubator/iterator-erl/actions/workflows/ci.yml)
[![License][license-image]][license-url]
[![Developed at Klarna][klarna-image]][klarna-url]


This library provides an interface to define your own "lazy sequences" and also provides a list of
helpers to operate on such sequences that mimicks the OTP stdlib [lists](https://www.erlang.org/doc/man/lists)
functions.
"lazy sequences" allows you to operate on potentially huge streams of data the same way you would
operate on them if they are completely loaded into memory as a very long list, but doing so only
using constant memory footprint. It achieves that by loading one-list-item at a time.

## Usage example

It's primary intention is that you could define your own "iterators" using our generic interface
and then use our helper functions to build the "processing" pipeline for the stream of data.

For example, let's implement an iterator that returns next line from a file each time:

```erlang
file_line_iterator(FileName) ->
    {ok, Fd} = file:open(Filename, [read, read_ahead, raw]),
    iterator:new(fun yield_file_line/1, Fd, fun close_file/1).

yield_file_line(Fd) ->
    case file:read_line(Fd) of
        {ok, Line} ->
            {Line, Fd};
        eof ->
            done;
        {error, Reason} ->
            error(Reason)
    end.

close_file(Fd) ->
    file:close(Fd).
```

You use `iterator:new/3` to create the (opaque) iterator structure, where

* 1st argument is the "yield" function - the function that returns either
  `{NextSequenceElement, NewState}` or atom `done` when sequence is exhausted
* 2nd argument is the initial state of the iterator (in this example it does not change)
* 3rd optional argument is the "close" garbage-collection function that will be called when
  iterator is exhausted or discarded (eg, used with `iterator:takewhile/2`)

After that you can use the primitive `iterator:next/1` function that returns either
`{ok, NextElement, NewIterator}` or `done`. But the real power comes when you build
a "processing pipeline" instead.

Then we may build a "processing pipeline" for this iterator. Let's say we want to filter-out the
lines that match a regular expression:

```erlang
LinesIterator = file_line_iterator("my_huge_file.txt"),
MatchingIterator =
    iterator:filter(
        fun(Line) ->
            case re:run(Line, "^[0-9]+$") of
                nomatch ->
                    false;
                {match, _} ->
                    true
            end
        end, LinesIterator).
```

And then we want to convert each matching line to integer

```erlang
IntegerIterator = iterator:map(fun erlang:binary_to_integer/1, MatchingIterator).
```

And finally we want to sum all the integers

```erlang
Sum = iterator:fold(fun(Int, Acc) -> Int + Acc end, 0, IntegerIterator).
```

The `iterator:fold/2` is different from other pipeline functions because it does not return
the new iterator, but it "forces" the execution of iterator by reading inner iterator's elements
one-by-one and applying `fun` to them, maintaining the `Acc` state.
Another such functions are `iterator:to_list/1`, `iterator:search/2`, `iterator:foreach/2`.

With this code, using `iterator`, we managed to go through the whole file never keeping more than
a single line in-memory but were able to work with it using the same code style and high-order
functions as what we would use if we read all the file lines in memory.

Full list of helper functions see in the [`iterator.erl`](src/iterator.erl) or in
[HEX docs](https://hexdocs.pm/iterator/iterator.html). But the naming is the same as in the
OTP `lists` module. There are several differences however:

* `iterator:chunks/2`
* `iterator:flatten1/1` like `lists:flatten/1`, but only goes 1 level deep like `flatmap`
* `iterator:mapfoldl/3` differs from `lists` version - it is rather a statefull `map` here

### Parallel processing

Functions `iterator_pmap:pmap/2` and `iterator_pmap:pmap/3` provide parallel version
of `iterator:map/2`: it takes iterator as input and returns a new iterator where map function
is executed for each input element in parallel on a pool of worker processes.
The `ordered` parameter controls if the parallel map should preserve the order of the original
iterator or it is allowed to reshuffle the elements (so it outputs elements which are processed
faster - earlier, increasing the throughput).

### Rate limiting

Functions `iterator_rate:leaky_bucket/2` and `iterator_rate:token_bucket/2` provides a
rate-limiter (shaper) pass-through iterator that makes sure that no more than X items can pass
through it in the time window. They call `timer:sleep/1` when the rate limit is exceeded.
See [Leaky Bucket](https://en.wikipedia.org/wiki/Leaky_bucket) and
[Token Bucket](https://en.wikipedia.org/wiki/Token_bucket) algorithms.
The difference is that token bucket allows short bursts of up to `capacity` after pauses
(amortized rate), while leaky bucket NEVER exceeds the rate.
Our implementations do not add all tokens at once, but just sleeps up to `window_ms / rate` at
once if necessary. Each item in the iterator consumes exactly one token.

The following code would keep the rate of the items passing through it at no more than
30 per-second, allowing short-term bursts after pauses up to 60 items.

```erlang
I0 = ...,
I1 = iterator_rate:token_bucket(
  #{
    rate => 30,
    capacity => 60,
    window_ms => 1000
   },
   I0),
...

```

### Progress reporting

Another non-standard function is `pv/3` (from `man pv` - "pipe view"). A pass-through iterator
that can be added somewhere in the pipeline to periodically (either every `for_each_n` elements
or every `every_s` seconds) report the current progress of a long-running iterator:

```erlang
I0 = ...,
I1 = iterator:pv(
  fun(SampleElement, TimePassed, ItemsPassed, TotalItems) ->
    TimeS = erlang:convert_time_unit(TimePassed, native, second),
    ?LOG_INFO("Processed ~p items. Pace is ~p per-second. Current item: ~p",
              [TotalItems, ItemsPassed / TimeS, SampleElement])
  end,
  #{for_each_n => 1000,
    every_s => 30},
  I0),
...
```
This example will log current progress either every 30 seconds or after processing every 1000
elements (whichever triggers first).

If you don't like the name `pv`, there is an alias named `iterator:report/3`.

## Setup

Add it to your `rebar.config`

```erlang
{deps, [iterator]}.
```

## Release History

See in the GitHub releases section.

## License

Copyright © 2023 Klarna Bank AB

For license details, see the [LICENSE](LICENSE) file in the root of this project.


<!-- Markdown link & img dfn's -->
[ci-image]: https://img.shields.io/badge/build-passing-brightgreen?style=flat-square
[ci-url]: https://github.com/klarna-incubator/TODO
[license-image]: https://img.shields.io/badge/license-Apache%202-blue?style=flat-square
[license-url]: http://www.apache.org/licenses/LICENSE-2.0
[klarna-image]: https://img.shields.io/badge/%20-Developed%20at%20Klarna-black?style=flat-square&labelColor=ffb3c7&logo=klarna&logoColor=black
[klarna-url]: https://klarna.github.io
