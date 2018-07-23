# registers-sml
Statistical Method Library for Registers

Methods are written in Scala Spark but also exposed via wrappers so that they can be called within:
 - Scala Spark
 - Java Spark
 - PySpark
 - Sparklyr

 Tests are provided, using the same test datasets, for all lanaguages

## Dependencies

### Java/Scala
Install Maven:
```shell
brew install maven
```

### Python
Install behave for testing
```shell
pip install behave
```

Install pyspark dep:
```shell
pip install pyspark
```

### R
Install devtools for testing:
```shell
install.packages(“devtools”)
```

## Building the Methods
```shell
mvn package
```


## Running the Tests

### Java/Scala
```shell
mvn test
```

### Python
```shell
behave
```

### R
```shell
devtools::test()
```


