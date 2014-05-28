# Dynohub, a Clojure DynamoDB client

Dynohub was forked from Peter Taoussanis' [Faraday](https://github.com/ptaoussanis/faraday).

The most of internal details are same between Dynohub and Faraday, except:
  * Experimental new design to learn low level Clojure programming
  * Further abstraction layers (dynolite)
  * Flexible binary data read/write

Most parts of Faraday documentation can be served as Dynohub documentation.

## Getting started

### Dependencies

Add the necessary dependency to your [Leiningen][] `project.clj` and `require` the library in your ns:

```clojure
[com.ozjongwon/dynohub "1.0.0-RC2"] ; project.clj
(ns my-app (:require [ozjongwon.dynohub :as dh])) ; ns
```

### Preparing a database

See Faraday's [Preparing a database](https://github.com/ptaoussanis/faraday#preparing-a-database)

### Connecting

See Faraday's [Connecting](https://github.com/ptaoussanis/faraday#connecting)

### Examples

There are test cases in dynohub/test/dynohub/dynohub.clj

## License

Copyright &copy; 2014 Jong-won Choi. Distributed under the [Eclipse Public License][], the same as Clojure.



[Eclipse Public License]: <https://raw2.github.com/ozjongwon/dynohub/master/LICENSE>