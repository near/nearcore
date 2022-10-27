# Delay Detector

Delay Detector is a library that can be used to measure time spent in different functions.

Internally it measures time that passed between its creation and descruction.

## Example
```
{
    let d_ = DelayDetector::new("my function");
    my_function();
    // d_ goes out of scope and prints the time information into the log.
}

```

More advanced example:

```
{
    let d = DelayDetector::new("long computation");
    part1();
    d.snaphot("part1")
    part2();
    d.snapshot("part2")
    part3();
    d.shapshot("part3")
    // d goes out of scope and prints the total time information and time between each 'snapshot' call.
}
```
