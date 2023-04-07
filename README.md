fork from [Dapper.SimpleCRUD](https://github.com/ericdc1/Dapper.SimpleCRUD) and support  multi-type databases .

The default dialect is SQLServer but you can *assign* it like this:

>conn.Get(1, dialect: Dialect.MySQL)

They're thread safety.
