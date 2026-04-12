package script

//go:generate ./generate.sh ../internal/spark/parser --visitor
//go:generate ./generate.sh ../internal/hive/parser
//go:generate ./generate.sh ../internal/starrocks/parser StarRocks.g4 --fix-starrocks-parser --visitor
//go:generate ./generate.sh ../internal/mysql/parser --fix-mysql-parser
