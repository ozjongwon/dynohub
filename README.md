# Dynohub, a Clojure DynamoDB client with transaction layer

Dynohub was initially forked from Peter Taoussanis' [Faraday](https://github.com/ptaoussanis/faraday).
Then added a transaction layer(a port of https://github.com/awslabs/dynamodb-transactions) on top of the plain client API.

There are three layers of abstraction:
  * Dynohub  - a plain client layer
  * Dynolite - a handy client layer on top of Dynohub
  * Dynotx   - a transaction enabled client layer on top of Dynolite

(NOTE: All the limits of Amazon's transaction library also applied to Dynohub's transaction layer, Dynotx)

The most of internal details are same between Dynohub and Faraday, except:
  * Attribute name and value aliasing for advanced filtering (aws-java-sdk 1.9.0+). See [Examples](https://github.com/ozjongwon/dynohub#examples)


  * Experimental new design to learn low level Clojure programming
  * Further abstraction layers (Dynolite)
  * Flexible binary data read/write
  * A transaction layer

Dynotx provides 'with-transaction' macro to simplify and hide low level details. It provides the same consistent API functions as Dynolite.

Currently there is no documentation, but most parts of Faraday documentation can be served as Dynohub documentation.
Also there are some test cases which may be useful to use the Dynotx layer.

## Getting started

### Dependencies

Add the necessary dependency to your [Leiningen][] `project.clj` and `require` the library in your ns:

```clojure
[com.ozjongwon/dynohub "1.1.0"]              ; project.clj

(ns my-app (:require [ozjongwon.dynohub :as dh]  ; low level interface
    	   	     [ozjongwon.dynolite :as dl] ; high level interface
    	   	     [ozjongwon.dynotx :as dt]   ; high level interface with transaction
```

### Preparing a database

See Faraday's [Preparing a database](https://github.com/ptaoussanis/faraday#preparing-a-database)

### Connecting

See Faraday's [Connecting](https://github.com/ptaoussanis/faraday#connecting)

### Examples

There are test cases in dynohub/test/dynohub/dynohub.clj

#### Attribute name and value aliasing for advanced filtering

With new AWS Java SDK realase(aws-java-sdk 1.9.0), Amazon added **filter expression**.
Avaliable comparators and functions are := :<> :< :<= :> :>= :and :or :not :exists :not-exists :begins-with :contains :between :in
See [Amazon's conditional expression reference](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference).


Dynohub supports filter expression using alias-attr-name-map, alias-attr-value-map and filter-exp:

```clojure
(dl/query :employee {:site-id [:eq "4w"]}
              :index :family-name-index
              :alias-attr-name-map {:fn :firstName      :sn :familyName         :e :emailAddress
                                    :p :phoneNumber     :d :dateEmployment      :t :terminatedP         :a :active?}
              :alias-attr-value-map {:active true}
              :projection-attrs [:fn :sn :e :p :d :t :id] ;; :alias-attr-name-map also can be used in :projection-attrs
              :filter-exp [:= :a :active])
```


## License

Copyright &copy; 2014 Jong-won Choi. Distributed under the [Eclipse Public License][], the same as Clojure.



[Eclipse Public License]: <https://raw2.github.com/ozjongwon/dynohub/master/LICENSE>