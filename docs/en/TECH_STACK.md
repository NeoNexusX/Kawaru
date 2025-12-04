# Collaboration Guide

First, familiarize yourself with Git. You can refer to:

https://git-scm.com/book/en/v2

https://www.w3schools.com/git/default.asp?remote=github

Choose one of the above to read. You should at least understand the following concepts:

![image-20251022004123599](https://s2.loli.net/2025/10/22/rsv8KWFYU7pVdiJ.png)

## Python

Quick start for Python:

https://docs.python.org/3/tutorial/index.html

You can choose any course, but it must cover at least the following content:

### Python Basic Knowledge Requirements

#### 1. Python Basics
- **Basic Data Structures**:
  - `list`: Indexing, slicing, `append`, `extend`, `remove`
  - `dict`: Key-value pairs, `get`, `keys`, `values`, `items`
  - `tuple`: Immutable sequences
  - `set`: Deduplication, union, intersection, difference
- **String Operations**: Formatting (f-string), `split`, `join`, common methods

#### 2. Functions and Modules
- **Function Definition**: `def`, parameters (positional, default, `*args`, `**kwargs`)
- **Return Values**: `return` statement
- **Module Import**:
  - `import module`
- **Package Management**: `pip install`, `requirements.txt`

#### 3. Object-Oriented Programming (Focus)
- **Class Basics**:
  - `class` keyword to define classes
  - `__init__` constructor method
  - Instance attributes vs. Class attributes
  - Instance methods, Class methods (`@classmethod`), Static methods (`@staticmethod`)
- **Inheritance and Polymorphism**:
  - Single inheritance, Multiple inheritance
  - `super()` to call parent class methods
  - Method overriding
- **Encapsulation**:
  - Private attributes (`_variable` and `__variable`)
  - `@property` decorator for getter/setter
- **Magic Methods**:
  - `__str__`, `__repr__`: String representation of objects
  - `__len__`, `__getitem__`: Support for `len()` and indexing operations

#### 4. Advanced Features
- **List Comprehensions**: `[x for x in range(10) if x % 2 == 0]`
- **Lambda Functions**: `lambda x: x * 2`
- **Exception Handling**:
  - `try-except-finally` structure
  - Catching specific exception types
  - Custom exception classes
- **File Operations**:
  - `open()` function and file modes ('r', 'w', 'a')
  - `with` statement (Context Manager)
  - Reading and writing text and binary files

## Backend

### FastAPI Tutorial

https://fastapi.tiangolo.com/tutorial/

Read the code and ensure you have no questions.

### Database and Storage

https://sqlmodel.tiangolo.com/

https://docs.sqlalchemy.org/en/20/

### Cache Redis

https://redis.io/docs/latest/get-started/

### Distributed Storage Scalability

MinIO, HDFS, S3

## Reference Platforms

- MetaSpace:
  - GitHub: https://github.com/metaspace2020/metaspace
  - Web Server: https://metaspace2020.org/
