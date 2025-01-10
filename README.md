# MapReduce in C

This project is a basic map reduce framework that can use threads to speed up excecution.
## Features

- **Map**: From this input `("1", "the quick brown fox")`, the map stage produces `("the", "1")`, `("quick", "1")`, `("brown", "1")`, and `("fox", "1")` as the output. From These are essentially recording the encounters of these words from the input.
- **Group-by-key**: For the unique key "quick", the group-by-key stage sees that there are two values associated with it and groups them as follows: `("quick", ["1", "1"])`
- **Reduce**: Turns `("quick", ["1", "1"])` to `("quick", "2")`

### Complete Outputs

Input:
```
("1", "the quick brown fox")
("2", "jumps over the lazy dog")
("3", "the quick brown fox")
("4", "jumps over the lazy dog")
```

What the map stage produces:
```
("the", "1")
("quick", "1")
("brown", "1")
("fox", "1")
("jumps", "1")
("over", "1")
("the", "1")
("lazy", "1")
("dog", "1")
("the", "1")
("quick", "1")
("brown", "1")
("fox", "1")
("jumps", "1")
("over", "1")
("the", "1")
("lazy", "1")
("dog", "1")
```

What the group-by-key stage produces:
```
("the", ["1", "1", "1", "1"])
("quick", ["1", "1"])
("brown", ["1", "1"])
("fox", ["1", "1"])
("jumps", ["1", "1"])
("over", ["1", "1"])
("lazy", ["1", "1"])
("dog", ["1", "1"])
```

What the reduce stage produces, which is the final output of MapReduce:
```
("the", "4")
("quick", "2")
("brown", "2")
("fox", "2")
("jumps", "2")
("over", "2")
("lazy", "2")
("dog", "2")
```









