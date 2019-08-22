# Homework projects of PNA

[![CircleCI Status]][circle]

This repository contains my homework projects of **[Practical Networked Applications(PNA) in Rust][PNA]** which is a part of PingCAP Talent Plan. The structures and test cases are adapted from PNA.

#### Project 1:

[PNA doc][pna_project1]|[tag][project1]

**Task:** Create an in-memory key/value store that passes simple tests and responds
to command-line arguments.

**Goals:**

- [x] Install the Rust compiler and tools
- [x] Learn the project structure used throughout this course
- [x] Use `cargo init` / `run` / `test` / `clippy` / `fmt`
- [x] Learn how to find and import crates from [crates.io][crates.io]
- [x] Define an appropriate data type for a key-value store
- [x] Limit the size of key up to 256B, the size of Value up to 4KB

#### Project 2:

[PNA doc][pna_project2]|[tag][project2]

**Task:** Create a persistent key/value store that can be accessed from the
command line.

**Goals:**

- [x] Handle and report errors robustly
- [x] Use serde for serialization
- [x] Write data to disk as a log using standard file APIs
- [x] Read the state of the key/value store from disk
- [x] Map in-memory key-indexes to on-disk values
- [x] Periodically compact the log to remove stale data



#### Project 3:

[PNA doc][pna_project3]|[tag][project3]

**Task:** Create a single-threaded, persistent key/value store server and client
with synchronous networking over a custom protocol.

**Goals:**

- [x] Create a client-server application
- [x] Write a custom protocol with `std` networking APIs
- [x] Introduce logging to the server
- [x] Implement pluggable backends with traits
- [x] Benchmark the hand-written backend against `sled`



#### Project 4:

[PNA doc][pna_project4]|[tag][project4]

**Task:** Create a multithreaded, persistent key/value store server and client
with synchronous networking over a custom protocol.

**Goals:** 

- [x] Write a simple thread pool
- [x] Use channels for cross-thread communication
- [x] Share data structures with locks
- [ ] Perform read operations without locks
- [ ] Benchmark single-threaded vs multithreaded



[CircleCI Status]: https://circleci.com/gh/XDXX/PNA.svg?style=svg
[circle]: https://circleci.com/gh/XDXX/PNA
[PNA]: https://github.com/pingcap/talent-plan/tree/master/rust
[project1]: https://github.com/XDXX/PNA/tree/69a9bc1f59895c65937f0fd441c3943f2505ce08
[project2]: https://github.com/XDXX/PNA/tree/4b2a98872d7855e501905ceaadb6b5fb59b3d7f9
[project3]: https://github.com/XDXX/PNA/tree/aafa6de64c833948c93bfb3fbcf5445b6f59c841
[project4]: https://github.com/XDXX/PNA/tree/1f8c0a818599c18d9efa4fc8f7c1d819d2343f0e
[pna_project1]: https://github.com/pingcap/talent-plan/blob/master/rust/projects/project-1/project.md
[pna_project2]: https://github.com/pingcap/talent-plan/blob/master/rust/projects/project-2/project.md
[pna_project3]: https://github.com/pingcap/talent-plan/blob/master/rust/projects/project-3/project.md
[pna_project4]: https://github.com/pingcap/talent-plan/blob/master/rust/projects/project-4/project.md