# MySQL SQL Parser

Adaptation of
- [https://github.com/antlr/grammars-v4/sql/mysql/Oracle/MySQLLexer.g4](https://github.com/antlr/grammars-v4/blob/master/sql/mysql/Oracle/MySQLLexer.g4)
- [https://github.com/antlr/grammars-v4/sql/mysql/Oracle/MySQLParser.g4](https://github.com/antlr/grammars-v4/blob/master/sql/mysql/Oracle/MySQLParser.g4)

fixed for Go by referencing 
- [transformGrammar.py](https://github.com/antlr/grammars-v4/blob/master/sql/mysql/Oracle/Go/transformGrammar.py)
- [MySQLLexerBase.go](https://github.com/antlr/grammars-v4/tree/master/sql/mysql/Oracle/Go/MySQLLexerBase.go)
- [MySQLParserBase.go](https://github.com/antlr/grammars-v4/tree/master/sql/mysql/Oracle/Go/MySQLParserBase.go)
- [SqlMode.go](https://github.com/antlr/grammars-v4/tree/master/sql/mysql/Oracle/Go/SqlMode.go)
- [SqlModes.go](https://github.com/antlr/grammars-v4/tree/master/sql/mysql/Oracle/Go/SqlModes.go)
