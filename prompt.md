# build parsers for dependency analyzing

## what's dependency analyzing

there is single statement sql or multiple statement sql. we want to parse it and known what tables it reads and writes. this process is called dependency analyzing.

## your jobs

you should implement `DependencyAnalyzer` for each database engine.
- the implementation should be in `internal/xxx_engine`, eg. [`internal/mysql`](./internal/mysql)
- reads README.md in implementation directory for more details. maybe grab antlr files. maybe use an open source parser. maybe should transform files after antlr code generation
- implements `DependencyAnalyzer` interface defined in [`dependency_analyzer.go`](./analyzer/dependency_analyzer.go)
