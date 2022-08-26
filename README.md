# StructuredDB

This is a super basic example of a structured in-memory database written in pure C. Supports inserts and selects with a primary "id" key and indexed columns. No joins yet...that starts to get very complicated. This is just a learning experience/example, no practical purpose.

This sounds very simple, but this involves writing a hash table implementation for looking up column names, manually allocating and copying memory for maximum query efficiency, and building macros to handle varying data types.

I'm also writing an UnstructuredDB to compare implementation and speed.
