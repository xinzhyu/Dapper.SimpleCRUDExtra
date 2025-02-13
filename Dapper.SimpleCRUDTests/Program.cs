﻿using System;
using System.Data.SqlClient;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using IBM.Data.DB2.Core;
using MySql.Data.MySqlClient;
using Npgsql;

namespace Dapper.SimpleCRUDTests
{
    class Program
    {
        public const string SQLServerName = @".\sqlexpress";

        static void Main()
        {
            Setup();
            RunTests();

            SetupSqLite();
            RunTestsSqLite();

            //PostgreSQL tests assume port 5432 with username postgres and password postgrespass
            //they are commented out by default since postgres setup is required to run tests
            //SetupPg(); 
            //RunTestsPg();   

            //MySQL tests assume port 3306 with username admin and password admin
            //they are commented out by default since mysql setup is required to run tests
            //SetupMySQL();
            //RunTestsMySQL();

            //DB2 tests assume port 50000 with username db2admin and password db2admin
            //they are commented out by default since db2 setup is required to run tests
            //SetupDB2();
            //RunTestsDB2();
        }

        private static void Setup()
        {
            using (var connection = new SqlConnection($@"Data Source={SQLServerName};Initial Catalog=Master;Integrated Security=True"))
            {
                connection.Open();
                try
                {
                    connection.Execute(@" DROP DATABASE DapperSimpleCrudTestDb; ");
                }
                catch (Exception)
                { }

                connection.Execute(@" CREATE DATABASE DapperSimpleCrudTestDb; ");
            }

            using (var connection = new SqlConnection($@"Data Source ={SQLServerName};Initial Catalog=DapperSimpleCrudTestDb;Integrated Security=True"))
            {
                connection.Open();
                connection.Execute(@" create table Users (Id int IDENTITY(1,1) not null, Name nvarchar(100) not null, Age int not null, ScheduledDayOff int null, CreatedDate datetime DEFAULT(getdate())) ");
                connection.Execute(@" create table Car (CarId int IDENTITY(1,1) not null, Id int null, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" create table BigCar (CarId bigint IDENTITY(2147483650,1) not null, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" create table City (Name nvarchar(100) not null, Population int not null) ");
                connection.Execute(@" CREATE SCHEMA Log; ");
                connection.Execute(@" create table Log.CarLog (Id int IDENTITY(1,1) not null, LogNotes nvarchar(100) NOT NULL) ");
                connection.Execute(@" CREATE TABLE [dbo].[GUIDTest]([Id] [uniqueidentifier] NOT NULL,[name] [varchar](50) NOT NULL, CONSTRAINT [PK_GUIDTest] PRIMARY KEY CLUSTERED ([Id] ASC))");
                connection.Execute(@" create table StrangeColumnNames (ItemId int IDENTITY(1,1) not null Primary Key, word nvarchar(100) not null, colstringstrangeword nvarchar(100) not null, KeywordedProperty nvarchar(100) null)");
                connection.Execute(@" create table UserWithoutAutoIdentity (Id int not null Primary Key, Name nvarchar(100) not null, Age int not null) ");
                connection.Execute(@" create table IgnoreColumns (Id int IDENTITY(1,1) not null Primary Key, IgnoreInsert nvarchar(100) null, IgnoreUpdate nvarchar(100) null, IgnoreSelect nvarchar(100)  null, IgnoreAll nvarchar(100) null) ");
                connection.Execute(@" CREATE TABLE GradingScale ([ScaleID] [int] IDENTITY(1,1) NOT NULL, [AppID] [int] NULL, [ScaleName] [nvarchar](50) NOT NULL, [IsDefault] [bit] NOT NULL)");
                connection.Execute(@" CREATE TABLE KeyMaster ([Key1] [int] NOT NULL, [Key2] [int] NOT NULL, CONSTRAINT [PK_KeyMaster] PRIMARY KEY CLUSTERED ([Key1] ASC, [Key2] ASC))");
                connection.Execute(@" CREATE TABLE [dbo].[stringtest]([stringkey] [varchar](50) NOT NULL,[name] [varchar](50) NOT NULL, CONSTRAINT [PK_stringkey] PRIMARY KEY CLUSTERED ([stringkey] ASC))");

            }
            Console.WriteLine("Created database");
        }

        private static void SetupPg()
        {
            using (var connection = new NpgsqlConnection(String.Format("Server={0};Port={1};User Id={2};Password={3};Database={4};", "localhost", "5432", "postgres", "postgrespass", "postgres")))
            {
                connection.Open();
                // drop  database 
                connection.Execute("DROP DATABASE IF EXISTS  testdb;");
                connection.Execute("CREATE DATABASE testdb  WITH OWNER = postgres ENCODING = 'UTF8' CONNECTION LIMIT = -1;");
            }
            System.Threading.Thread.Sleep(1000);

            using (var connection = new NpgsqlConnection(String.Format("Server={0};Port={1};User Id={2};Password={3};Database={4};", "localhost", "5432", "postgres", "postgrespass", "testdb")))
            {
                connection.Open();
                connection.Execute(@" create table Users (Id SERIAL PRIMARY KEY, Name varchar not null, Age int not null, ScheduledDayOff int null, CreatedDate date not null default CURRENT_DATE) ");
                connection.Execute(@" create table Car (CarId SERIAL PRIMARY KEY, Id int null, Make varchar not null, Model varchar not null) ");
                connection.Execute(@" create table BigCar (CarId BIGSERIAL PRIMARY KEY, Make varchar not null, Model varchar not null) ");
                connection.Execute(@" alter sequence bigcar_carid_seq RESTART WITH 2147483650");
                connection.Execute(@" create table City (Name varchar not null, Population int not null) ");
                connection.Execute(@" CREATE SCHEMA Log; ");
                connection.Execute(@" create table Log.CarLog (Id SERIAL PRIMARY KEY, LogNotes varchar NOT NULL) ");
                connection.Execute(@" CREATE TABLE GUIDTest(Id uuid PRIMARY KEY,name varchar NOT NULL)");
                connection.Execute(@" create table StrangeColumnNames (ItemId Serial PRIMARY KEY, word varchar not null, colstringstrangeword varchar, keywordedproperty varchar) ");
                connection.Execute(@" create table UserWithoutAutoIdentity (Id int PRIMARY KEY, Name varchar not null, Age int not null) ");

            }

        }

        private static void SetupSqLite()
        {
            File.Delete(Directory.GetCurrentDirectory() + "\\MyDatabase.sqlite");
            SQLiteConnection.CreateFile("MyDatabase.sqlite");
            var connection = new SQLiteConnection("Data Source=MyDatabase.sqlite;Version=3;");
            using (connection)
            {
                connection.Open();
                connection.Execute(@" create table Users (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name nvarchar(100) not null, Age int not null, ScheduledDayOff int null, CreatedDate datetime default current_timestamp ) ");
                connection.Execute(@" create table Car (CarId INTEGER PRIMARY KEY AUTOINCREMENT, Id INTEGER null, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" create table BigCar (CarId INTEGER PRIMARY KEY AUTOINCREMENT, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" insert into BigCar (CarId,Make,Model) Values (2147483649,'car','car') ");
                connection.Execute(@" create table City (Name nvarchar(100) not null, Population int not null) ");
                connection.Execute(@" CREATE TABLE GUIDTest([Id] [uniqueidentifier] NOT NULL,[name] [varchar](50) NOT NULL, CONSTRAINT [PK_GUIDTest] PRIMARY KEY  ([Id] ASC))");
                connection.Execute(@" create table StrangeColumnNames (ItemId INTEGER PRIMARY KEY AUTOINCREMENT, word nvarchar(100) not null, colstringstrangeword nvarchar(100) not null, KeywordedProperty nvarchar(100) null) ");
                connection.Execute(@" create table UserWithoutAutoIdentity (Id INTEGER PRIMARY KEY, Name nvarchar(100) not null, Age int not null) ");
                connection.Execute(@" create table IgnoreColumns (Id INTEGER PRIMARY KEY AUTOINCREMENT, IgnoreInsert nvarchar(100) null, IgnoreUpdate nvarchar(100) null, IgnoreSelect nvarchar(100)  null, IgnoreAll nvarchar(100) null) ");
                connection.Execute(@" CREATE TABLE KeyMaster (Key1 INTEGER NOT NULL, Key2 INTEGER NOT NULL, PRIMARY KEY ([Key1], [Key2]))");
                connection.Execute(@" CREATE TABLE stringtest (stringkey nvarchar(50) NOT NULL,name nvarchar(50) NOT NULL, PRIMARY KEY ([stringkey] ASC))");

            }
        }

        private static void SetupMySQL()
        {
            using (var connection = new MySqlConnection(String.Format("Server={0};Port={1};User Id={2};Password={3};Database={4};", "localhost", "3306", "root", "admin", "sys")))
            {
                connection.Open();
                // drop  database 
                connection.Execute("DROP DATABASE IF EXISTS testdb;");
                connection.Execute("CREATE DATABASE testdb;");
            }
            System.Threading.Thread.Sleep(1000);

            using (var connection = new MySqlConnection(String.Format("Server={0};Port={1};User Id={2};Password={3};Database={4};", "localhost", "3306", "root", "admin", "testdb")))
            {
                connection.Open();
                connection.Execute(@" create table Users (Id INTEGER PRIMARY KEY AUTO_INCREMENT, Name nvarchar(100) not null, Age int not null, ScheduledDayOff int null, CreatedDate datetime default current_timestamp ) ");
                connection.Execute(@" create table Car (CarId INTEGER PRIMARY KEY AUTO_INCREMENT, Id INTEGER null, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" create table BigCar (CarId BIGINT PRIMARY KEY AUTO_INCREMENT, Make nvarchar(100) not null, Model nvarchar(100) not null) ");
                connection.Execute(@" insert into BigCar (CarId,Make,Model) Values (2147483649,'car','car') ");
                connection.Execute(@" create table City (Name nvarchar(100) not null, Population int not null) ");
                connection.Execute(@" CREATE TABLE GUIDTest(Id CHAR(38) NOT NULL,name varchar(50) NOT NULL, CONSTRAINT PK_GUIDTest PRIMARY KEY (Id ASC))");
                connection.Execute(@" create table StrangeColumnNames (ItemId INTEGER PRIMARY KEY AUTO_INCREMENT, word nvarchar(100) not null, colstringstrangeword nvarchar(100) not null, KeywordedProperty nvarchar(100) null) ");
                connection.Execute(@" create table UserWithoutAutoIdentity (Id INTEGER PRIMARY KEY, Name nvarchar(100) not null, Age int not null) ");
                connection.Execute(@" create table IgnoreColumns (Id INTEGER PRIMARY KEY AUTO_INCREMENT, IgnoreInsert nvarchar(100) null, IgnoreUpdate nvarchar(100) null, IgnoreSelect nvarchar(100)  null, IgnoreAll nvarchar(100) null) ");
                connection.Execute(@" CREATE table KeyMaster (Key1 INTEGER NOT NULL, Key2 INTEGER NOT NULL, CONSTRAINT PK_KeyMaster PRIMARY KEY CLUSTERED (Key1 ASC, Key2 ASC))");
            }

        }


        private static void RunTests()
        {
            var stopwatch = Stopwatch.StartNew();
            var sqltester = new Tests(Dialect.SQLServer);
            foreach (var method in typeof(Tests).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                var testwatch = Stopwatch.StartNew();
                Console.Write("Running " + method.Name + " in sql server");
                method.Invoke(sqltester, null);
                testwatch.Stop();
                Console.WriteLine(" - OK! {0}ms", testwatch.ElapsedMilliseconds);
            }
            stopwatch.Stop();

            // Write result
            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);

            using (var connection = new SqlConnection($@"Data Source={SQLServerName};Initial Catalog=Master;Integrated Security=True"))
            {
                connection.Open();
                try
                {
                    //drop any remaining connections, then drop the db.
                    connection.Execute(@" alter database DapperSimpleCrudTestDb set single_user with rollback immediate; DROP DATABASE DapperSimpleCrudTestDb; ");
                }
                catch (Exception)
                { }
            }
            Console.Write("SQL Server testing complete.");
            Console.ReadKey();
        }

        private static void RunTestsPg()
        {
            var stopwatch = Stopwatch.StartNew();
            var pgtester = new Tests(Dialect.PostgreSQL);
            foreach (var method in typeof(Tests).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                var testwatch = Stopwatch.StartNew();
                Console.Write("Running " + method.Name + " in PostgreSQL");
                method.Invoke(pgtester, null);
                Console.WriteLine(" - OK! {0}ms", testwatch.ElapsedMilliseconds);
            }
            stopwatch.Stop();
            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);

            Console.Write("PostgreSQL testing complete.");
            Console.ReadKey();
        }

        private static void RunTestsSqLite()
        {
            var stopwatch = Stopwatch.StartNew();
            var pgtester = new Tests(Dialect.SQLite);
            foreach (var method in typeof(Tests).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                //skip schema tests
                if (method.Name.Contains("Schema")) continue;
                var testwatch = Stopwatch.StartNew();
                Console.Write("Running " + method.Name + " in SQLite");
                method.Invoke(pgtester, null);
                Console.WriteLine(" - OK! {0}ms", testwatch.ElapsedMilliseconds);
            }
            stopwatch.Stop();
            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);
            Console.Write("SQLite testing complete.");
            Console.ReadKey();
        }

        private static void RunTestsMySQL()
        {
            var stopwatch = Stopwatch.StartNew();
            var mysqltester = new Tests(Dialect.MySQL);
            foreach (var method in typeof(Tests).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                //skip schema tests
                if (method.Name.Contains("Schema")) continue;
                if (method.Name.Contains("Guid")) continue;

                var testwatch = Stopwatch.StartNew();
                Console.Write("Running " + method.Name + " in MySQL");
                method.Invoke(mysqltester, null);
                Console.WriteLine(" - OK! {0}ms", testwatch.ElapsedMilliseconds);
            }
            stopwatch.Stop();
            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);

            Console.Write("MySQL testing complete.");
            Console.ReadKey();
        }
        private static void SetupDB2()
        {
            System.Security.SecureString secureString = new System.Security.SecureString();
            ProcessStartInfo processStartInfo;
            string user = "db2admin";

            foreach (char c in user)
            {
                secureString.AppendChar(c);
            }

            processStartInfo = new ProcessStartInfo();
            processStartInfo.FileName = @"db2.exe";
            processStartInfo.UserName = user;
            processStartInfo.Password = secureString;
            processStartInfo.EnvironmentVariables["DB2CLP"] = "DB20FADE";
            processStartInfo.UseShellExecute = false;
            processStartInfo.WorkingDirectory = @"C:\Program Files\IBM\SQLLIB\bin";

            foreach (string command in (new string[] { "drop database testdb", "create database testdb" }))
            {
                Process process = new Process();

                processStartInfo.Arguments = command;

                process.StartInfo = processStartInfo;

                process.Start();
                process.WaitForExit();
            }

            using (var connection = new DB2Connection(string.Format("Server={0};UID={1};PWD={2};Database={3};", "localhost:50000", user, user, "testdb")))
            {
                connection.Open();

                // Use the alias for pass the delete test.
                connection.Execute(@"create table ""Users"" (Id int not null generated by default as identity, Name varchar(100) not null, Age int not null, ScheduledDayOff int, CreatedDate date not null default current_date, primary key(Id));");
                connection.Execute(@"create alias users for ""Users""");
                connection.Execute(@"create table Car (CarId int not null generated by default as identity, Id int, Make varchar(100) not null, Model varchar(100) not null, primary key(CarId));");
                connection.Execute(@"create table BigCar (CarId bigint not null generated by default as identity(start with 2147483650), Make varchar(100) not null, Model varchar(100) not null, primary key(CarId));");
                connection.Execute(@"create table City (Name varchar(100) not null, Population int not null);");
                connection.Execute(@"create schema ""Log"";");
                connection.Execute(@"create table ""Log"".""CarLog"" (Id int not null generated by default as identity, LogNotes varchar(100) not null, primary key(Id));");
                connection.Execute(@"create table GUIDTest(Id varchar(38) not null, name varchar(50) not null, primary key(Id));");
                connection.Execute(@"create table StrangeColumnNames (""ItemId"" int not null generated by default as identity, Word varchar(100) not null, ""colstringstrangeword"" varchar(100), ""KeywordedProperty"" varchar(100), primary key(""ItemId""))");
                connection.Execute(@"create table UserWithoutAutoIdentity (Id int not null generated by default as identity, Name varchar(100) not null, Age int not null, primary key(Id));");
                connection.Execute(@"create table IgnoreColumns (Id int not null generated by default as identity, IgnoreInsert varchar(100), IgnoreUpdate varchar(100), IgnoreSelect varchar(100), IgnoreAll varchar(100), primary key(Id));");
                connection.Execute(@"create table KeyMaster (Key1 int not null, Key2 int not null, PRIMARY KEY (Key1, Key2));");
                connection.Execute(@"create table stringtest (stringkey varchar(50) not null, name varchar(50) not null, primary key(stringkey))");
            }
        }

        private static void RunTestsDB2()
        {
            var stopwatch = Stopwatch.StartNew();
            var db2tester = new Tests(Dialect.DB2);
            foreach (var method in typeof(Tests).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
            {
                //skip schema tests
                if (method.Name.Contains("Schema")) continue;
                if (method.Name.Contains("Guid")) continue;
                var testwatch = Stopwatch.StartNew();
                Console.Write("Running " + method.Name + " in DB2");
                method.Invoke(db2tester, null);
                Console.WriteLine(" - OK! {0}ms", testwatch.ElapsedMilliseconds);
            }
            stopwatch.Stop();
            Console.WriteLine("Time elapsed: {0}", stopwatch.Elapsed);

            Console.Write("DB2 testing complete.");
            Console.ReadKey();
        }
    }
}
