# Scala Function Pipeline

Imagine you have a Scala Sequence, like:

```
val seq = Seq(1,2,3,4,5)
```

and you apply a few functions to it through `map`:

```
seq
  .map { _.toString }
  .map { _ + "!" }
  .map { "hi #" + _ }
```

Scala makes it really easy to run it faster with parallel collections:

```
seq
  .par
  .map { _.toString }
  .map { _ + "!" }
  .map { "hi #" + _ }
```

But what you get in the process of mapping all functions are several "copies" of that sequence in between:

```
seq
  .map { _.toString }     // Seq("1", "2", "3", "4")
  .map { _ + "!" }        // Seq("1!", "2!", "3!", "4!", "5!")
  .map { "hello #" + _ }  // Seq("hi #1!", "hi #2!", "hi #3!", "hi #4!", "hi #5!")
```


In this example, due to the creation of each intermediate sequence, there are a few disadvantages on memory usage. Plus, if you want to get an item processed as soon as possible and just pass it further (another system, DB, etc), that's not possible, since you must wait the whole computation to the finished.

For solving this, there are a few libraries like [scalaz-stream](https://github.com/scalaz/scalaz-stream) and [RxJava](https://github.com/Netflix/RxJava). This project aims to be something similar, but simpler (meaning: provide less functionality) and that allows you to control the parallelism on each intermediate step.

For instance, let's say one of the steps is quite slow (step **#2**). For simplicity, I'll use a `sleep` call, but that can be network access, etc:

```
seq
  .par
  .map { _.toString }                        // #1
  .map { s => Thread.sleep(3000); s + "!" }  // #2
  .map { "hi #" + _ }                        // #3
```

## How to use it

What this library enables you to do is to fine tune each step and define a parallelism level it should be run with:

```
val p = Pipeline[Int]
  .map { _.toString }
  .map(5) { s => Thread.sleep(3000); s + "!" }
  .map { "hi #" + _ }
```

As you can see, the code is quite similar in order to define the pipeline. Only a small change has been introduced on step **#2** to define it as parallelism level 5. Realize that we don't say 5 threads here, as underneath we use 5 [Akka](http://akka.io) actors, that might be or not mapped to 5 different threads.

For running the pipeline, there are two possible ways:

* A fire and forget style, where a callback object will be called when the computation of the value is finished

```
val cr = p.pipe(new Output[String] {
  def apply(s: String) = println(s"finished $s")
  def error(e: Exception) = e.printStackTrace
})
(0 until 10).foreach(cr.apply))
```

* A `Future` returning call:

```
val fr = p.pipe
val f = fr(100)
f.onSuccess { case v => println(s"success $v") }
```

## What happens

By doing the following: 

```
val p = Pipeline[Int]
  .map { _.toString }
  .map { _ + "!" }
  .map { "hi #" + _ }
  
val fr = p.pipe
```

We are creating a pipeline with 3 stages. Let's say, F1, F2, and F3.

Let's say we pass 5 different values in (1, 2, 3, 4, 5):

```
(1 to 5).foreach(fr.apply)
```

On each interaction of the pipeline, what you get is:

|     | F1   | F2   | F3   | Out      |
|:----|:-----|:-----|:-----|:---------|
| #00 |      |      |      |          |
| #01 | 1    |      |      |          |
| #02 | 2    | "1"  |      |          |
| #03 | 3    | "2"  | "1!" |          |
| #04 | 4    | "3"  | "2!" | "hi #1!" | 
| #05 | 5    | "4"  | "3!" | "hi #2!" |
| #05 |      | "5"  | "4!" | "hi #3!" |
| #06 |      |      | "5!" | "hi #4!" |
| #07 |      |      |      | "hi #5!" |
| #08 |      |      |      |          |

If you ever learned how processor pipelining works, that's pretty much the same thing.

Now, let's reuse the example where step **#2** might be slow and we increase the parallelism level of F2. What you might have is this:

|     | F1   | F2(5)                   | F3   | Out      |
|:----|:-----|:------------------------|:-----|:---------|
| #00 |      |                         |      |          |
| #01 | 1    |                         |      |          |
| #02 | 2    | "1"                     |      |          |
| #03 | 3    | "1", "2"                |      |          |
| #04 | 4    | "1", "2", "3"           |      |          | 
| #05 | 5    | "1", "2", "3", "4"      |      |          |
| #06 |      | "1", "2", "3", "4", "5" |      |          |
| #07 |      |      "2", "3", "4", "5" | "1!" |          |
| #08 |      |           "3", "4", "5" | "2!" | "hi #1!" |
| #10 |      |                "4", "5" | "3!" | "hi #2!" |
| #11 |      |                     "5" | "4!" | "hi #3!" |
| #12 |      |                         | "5!" | "hi #4!" |
| #13 |      |                         |      | "hi #5!" |
| #14 |      |                         |      |          |

Although the table above might not make it clear, but since F1 and F3 are quite quick steps, you end up having 5 Out's almost the same time. 

The final order might not be the same as the input, as each F2 instance might receive more CPU time than the other.