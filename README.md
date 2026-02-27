This project aims to address this issue by proposing the design and implementation of DIASIM,
a deterministic simulator for distributed algorithms.
In DIASIM, distributed algorithms are expressed as collections of local behaviors reacting to
messages and logical time events. The simulator is responsible for orchestrating execution, se
lecting which events occur and when, injecting failures, and recording detailed execution traces.
This clear separation of concerns ensures that algorithms remain simple and declarative, while
execution semantics are centralized, well-defined, and reproducible.
