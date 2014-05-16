# Dynohub, a Clojure DynamoDB client

Dynohub was forked from Peter Taoussanis' [Faraday](https://github.com/ptaoussanis/faraday).

The most of internal details are same between Dynohub and Faraday, except:
	* Experimental to learn low level Clojure programming
	* Further abstraction layers

Most parts of Faraday documentation can be served as Dynohub documentation.

## Getting started

### Dependencies

Add the necessary dependency to your [Leiningen][] `project.clj` and `require` the library in your ns:

```clojure
[com.pegotezzi/dynohub "0.0.1"] ; project.clj
(ns my-app (:require [pegotezzi.dynohub :as dh])) ; ns
```

### Preparing a database

See Faraday's [Preparing a database](https://github.com/ptaoussanis/faraday#preparing-a-database)

### Connecting

See Faraday's [Connecting](https://github.com/ptaoussanis/faraday#connecting)

## License

Copyright &copy; 2014 Jong-won Choi. Distributed under the [Eclipse Public License][], the same as Clojure.
