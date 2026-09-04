using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace EtlKimball
{
    public class Program
    {
        public static void Main(string[] args)
        {
            // Ruta al archivo ZIP
            string zipPath = "";

            // Ruta de la base de datos SQLite
            string dbName = "";

            // Verificamos que el archivo exista
            if (!File.Exists(zipPath))
            {
                Console.WriteLine($"Archivo no encontrado: {zipPath}");
                return;
            }

            Console.WriteLine("Extrayendo datos...");

            var (accounts, customers, transactions) =
                ExtractJsonFromZip(zipPath);

            Console.WriteLine("Transformando datos...");

            var result = TransformData(
                accounts,
                customers,
                transactions
            );

            Console.WriteLine("Creando tablas...");

            CreateTablesIfNotExist(dbName);

            Console.WriteLine("Cargando datos...");

            LoadToSqliteKimball(
                result.DimAccounts,
                result.DimCustomers,
                result.AccountCustomerMapping,
                result.DimDates,
                result.FactTransactions,
                dbName
            );

            Console.WriteLine("\nETL completado correctamente.");
        }


        // =====================================================
        // EXTRACT
        // =====================================================

        public static (
            List<Account>,
            List<Customer>,
            List<TransactionGroup>
        ) ExtractJsonFromZip(string zipPath)
        {
            List<Account> accounts = new();
            List<Customer> customers = new();
            List<TransactionGroup> transactions = new();

            using ZipArchive zip =
                ZipFile.OpenRead(zipPath);

            foreach (ZipArchiveEntry entry in zip.Entries)
            {
                string fileName = entry.FullName;

                if (!fileName.EndsWith(".json"))
                    continue;

                if (fileName.Contains("__MACOSX"))
                    continue;

                using Stream stream = entry.Open();

                string baseName =
                    Path.GetFileName(fileName);

                if (baseName ==
                    "sample_analytics.accounts.json")
                {
                    accounts =
                        JsonSerializer.Deserialize<List<Account>>(
                            stream
                        ) ?? new List<Account>();
                }
                else if (baseName ==
                         "sample_analytics.customers.json")
                {
                    customers =
                        JsonSerializer.Deserialize<List<Customer>>(
                            stream
                        ) ?? new List<Customer>();
                }
                else if (baseName ==
                         "sample_analytics.transactions.json")
                {
                    transactions =
                        JsonSerializer.Deserialize<
                            List<TransactionGroup>
                        >(stream)
                        ?? new List<TransactionGroup>();
                }
            }

            return (
                accounts,
                customers,
                transactions
            );
        }


        // =====================================================
        // TRANSFORM
        // =====================================================

        public static TransformResult TransformData(
            List<Account> accounts,
            List<Customer> customers,
            List<TransactionGroup> transactions
        )
        {
            var dimAccounts =
                new Dictionary<int, DimAccount>();

            int generatedAccountId = 1;

            // -------------------------
            // DIM ACCOUNTS
            // -------------------------

            foreach (var account in accounts)
            {
                dimAccounts[generatedAccountId] =
                    new DimAccount
                    {
                        AccountId =
                            generatedAccountId,

                        LimitAmount =
                            account.limit,

                        Products =
                            string.Join(
                                ",",
                                account.products
                                ?? new List<string>()
                            )
                    };

                // Guardamos el ID generado
                account.GeneratedAccountId =
                    generatedAccountId;

                generatedAccountId++;
            }


            // -------------------------
            // DIM CUSTOMERS
            // -------------------------

            var dimCustomers =
                new Dictionary<int, DimCustomer>();

            int nextCustomerId = 1;

            foreach (var customer in customers)
            {
                string username =
                    !string.IsNullOrEmpty(
                        customer.username
                    )
                    ? customer.username
                    : $"user{nextCustomerId}";

                string birthDate = "";

                try
                {
                    if (!string.IsNullOrEmpty(
                        customer.birthdate
                    ))
                    {
                        DateTime dt =
                            DateTime.Parse(
                                customer.birthdate,
                                null,
                                DateTimeStyles.RoundtripKind
                            );

                        birthDate =
                            dt.ToString("yyyy-MM-dd");
                    }
                }
                catch
                {
                    birthDate = "";
                }


                dimCustomers[nextCustomerId] =
                    new DimCustomer
                    {
                        CustomerId =
                            nextCustomerId,

                        Name =
                            customer.name ?? "",

                        Username =
                            username,

                        BirthDate =
                            birthDate,

                        Accounts =
                            customer.accounts
                            ?? new List<string>()
                    };

                nextCustomerId++;
            }


            // -------------------------
            // CUSTOMER - ACCOUNT
            // -------------------------

            var accountCustomerMapping =
                new List<AccountCustomer>();

            foreach (
                var customer
                in dimCustomers.Values
            )
            {
                foreach (
                    string accountId
                    in customer.Accounts
                )
                {
                    var account =
                        accounts.FirstOrDefault(
                            a =>
                            a.account_id ==
                            accountId
                        );

                    if (account != null)
                    {
                        accountCustomerMapping.Add(
                            new AccountCustomer
                            {
                                CustomerId =
                                    customer.CustomerId,

                                AccountId =
                                    account.GeneratedAccountId
                            }
                        );
                    }
                }
            }


            // -------------------------
            // DIM DATES
            // -------------------------

            var dimDates =
                new List<DimDate>();

            var dateIds =
                new Dictionary<string, int>();

            var factTransactions =
                new List<FactTransaction>();

            int nextDateId = 1;

            int transactionId = 1;


            // -------------------------
            // FACT TRANSACTIONS
            // -------------------------

            foreach (
                var transactionGroup
                in transactions
            )
            {
                string originalAccountId =
                    transactionGroup.account_id;

                var account =
                    accounts.FirstOrDefault(
                        a =>
                        a.account_id ==
                        originalAccountId
                    );

                if (account == null)
                    continue;

                int mappedAccountId =
                    account.GeneratedAccountId;

                int transactionCount =
                    transactionGroup.transaction_count;


                foreach (
                    var transaction
                    in transactionGroup.transactions
                )
                {
                    try
                    {
                        if (
                            string.IsNullOrEmpty(
                                transaction.date
                            )
                        )
                            continue;


                        DateTime dt =
                            DateTime.Parse(
                                transaction.date,
                                null,
                                DateTimeStyles.RoundtripKind
                            );

                        string dateKey =
                            dt.ToString("yyyy-MM-dd");


                        // Si la fecha no existe
                        if (
                            !dateIds.ContainsKey(
                                dateKey
                            )
                        )
                        {
                            dateIds[dateKey] =
                                nextDateId;

                            dimDates.Add(
                                new DimDate
                                {
                                    DateId =
                                        nextDateId,

                                    Date =
                                        dateKey
                                }
                            );

                            nextDateId++;
                        }


                        // Insertamos en FACT
                        factTransactions.Add(
                            new FactTransaction
                            {
                                TransactionId =
                                    transactionId,

                                AccountId =
                                    mappedAccountId,

                                DateId =
                                    dateIds[dateKey],

                                TransactionCount =
                                    transactionCount,

                                Amount =
                                    transaction.amount,

                                TransactionType =
                                    transaction.transaction_code
                                    ?? "",

                                Symbol =
                                    transaction.symbol
                                    ?? "",

                                Price =
                                    transaction.price,

                                Total =
                                    transaction.total
                            }
                        );

                        transactionId++;
                    }
                    catch
                    {
                        // Saltamos errores
                        continue;
                    }
                }
            }


            return new TransformResult
            {
                DimAccounts =
                    dimAccounts,

                DimCustomers =
                    dimCustomers,

                AccountCustomerMapping =
                    accountCustomerMapping,

                DimDates =
                    dimDates,

                FactTransactions =
                    factTransactions
            };
        }


        // =====================================================
        // CREATE TABLES
        // =====================================================

        public static void CreateTablesIfNotExist(
            string dbName
        )
        {
            using var connection =
                new SqliteConnection(
                    $"Data Source={dbName}"
                );

            connection.Open();

            string sql = @"

DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_dates;
DROP TABLE IF EXISTS account_customers;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_accounts;


CREATE TABLE dim_accounts (
    account_id INTEGER PRIMARY KEY,
    limit_amount REAL,
    products TEXT
);


CREATE TABLE dim_customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT,
    username TEXT,
    birth_date TEXT
);


CREATE TABLE account_customers (
    customer_id INTEGER,
    account_id INTEGER,
    PRIMARY KEY (
        customer_id,
        account_id
    )
);


CREATE TABLE dim_dates (
    date_id INTEGER PRIMARY KEY,
    date TEXT
);


CREATE TABLE fact_transactions (
    transaction_id INTEGER PRIMARY KEY,
    account_id INTEGER,
    date_id INTEGER,
    transaction_count INTEGER,
    amount REAL,
    transaction_type TEXT,
    symbol TEXT,
    price REAL,
    total REAL
);

";

            using var command =
                connection.CreateCommand();

            command.CommandText = sql;

            command.ExecuteNonQuery();
        }


        // =====================================================
        // LOAD
        // =====================================================

        public static void LoadToSqliteKimball(
            Dictionary<int, DimAccount> dimAccounts,
            Dictionary<int, DimCustomer> dimCustomers,
            List<AccountCustomer> accountCustomerMapping,
            List<DimDate> dimDates,
            List<FactTransaction> factTransactions,
            string dbName
        )
        {
            using var connection =
                new SqliteConnection(
                    $"Data Source={dbName}"
                );

            connection.Open();


            using var transaction =
                connection.BeginTransaction();


            // -------------------------
            // DIM ACCOUNTS
            // -------------------------

            foreach (
                var account
                in dimAccounts.Values
            )
            {
                using var command =
                    connection.CreateCommand();

                command.Transaction =
                    transaction;

                command.CommandText = @"
INSERT INTO dim_accounts
(
    account_id,
    limit_amount,
    products
)
VALUES
(
    @account_id,
    @limit_amount,
    @products
);
";

                command.Parameters.AddWithValue(
                    "@account_id",
                    account.AccountId
                );

                command.Parameters.AddWithValue(
                    "@limit_amount",
                    account.LimitAmount
                );

                command.Parameters.AddWithValue(
                    "@products",
                    account.Products
                );

                command.ExecuteNonQuery();
            }


            // -------------------------
            // DIM CUSTOMERS
            // -------------------------

            foreach (
                var customer
                in dimCustomers.Values
            )
            {
                using var command =
                    connection.CreateCommand();

                command.Transaction =
                    transaction;

                command.CommandText = @"
INSERT INTO dim_customers
(
    customer_id,
    name,
    username,
    birth_date
)
VALUES
(
    @customer_id,
    @name,
    @username,
    @birth_date
);
";

                command.Parameters.AddWithValue(
                    "@customer_id",
                    customer.CustomerId
                );

                command.Parameters.AddWithValue(
                    "@name",
                    customer.Name
                );

                command.Parameters.AddWithValue(
                    "@username",
                    customer.Username
                );

                command.Parameters.AddWithValue(
                    "@birth_date",
                    customer.BirthDate
                );

                command.ExecuteNonQuery();
            }


            // -------------------------
            // ACCOUNT CUSTOMERS
            // -------------------------

            foreach (
                var mapping
                in accountCustomerMapping
            )
            {
                using var command =
                    connection.CreateCommand();

                command.Transaction =
                    transaction;

                command.CommandText = @"
INSERT INTO account_customers
(
    customer_id,
    account_id
)
VALUES
(
    @customer_id,
    @account_id
);
";

                command.Parameters.AddWithValue(
                    "@customer_id",
                    mapping.CustomerId
                );

                command.Parameters.AddWithValue(
                    "@account_id",
                    mapping.AccountId
                );

                command.ExecuteNonQuery();
            }


            // -------------------------
            // DIM DATES
            // -------------------------

            foreach (
                var date
                in dimDates
            )
            {
                using var command =
                    connection.CreateCommand();

                command.Transaction =
                    transaction;

                command.CommandText = @"
INSERT INTO dim_dates
(
    date_id,
    date
)
VALUES
(
    @date_id,
    @date
);
";

                command.Parameters.AddWithValue(
                    "@date_id",
                    date.DateId
                );

                command.Parameters.AddWithValue(
                    "@date",
                    date.Date
                );

                command.ExecuteNonQuery();
            }


            // -------------------------
            // FACT TRANSACTIONS
            // -------------------------

            foreach (
                var fact
                in factTransactions
            )
            {
                using var command =
                    connection.CreateCommand();

                command.Transaction =
                    transaction;

                command.CommandText = @"
INSERT INTO fact_transactions
(
    transaction_id,
    account_id,
    date_id,
    transaction_count,
    amount,
    transaction_type,
    symbol,
    price,
    total
)
VALUES
(
    @transaction_id,
    @account_id,
    @date_id,
    @transaction_count,
    @amount,
    @transaction_type,
    @symbol,
    @price,
    @total
);
";

                command.Parameters.AddWithValue(
                    "@transaction_id",
                    fact.TransactionId
                );

                command.Parameters.AddWithValue(
                    "@account_id",
                    fact.AccountId
                );

                command.Parameters.AddWithValue(
                    "@date_id",
                    fact.DateId
                );

                command.Parameters.AddWithValue(
                    "@transaction_count",
                    fact.TransactionCount
                );

                command.Parameters.AddWithValue(
                    "@amount",
                    fact.Amount
                );

                command.Parameters.AddWithValue(
                    "@transaction_type",
                    fact.TransactionType
                );

                command.Parameters.AddWithValue(
                    "@symbol",
                    fact.Symbol
                );

                command.Parameters.AddWithValue(
                    "@price",
                    fact.Price
                );

                command.Parameters.AddWithValue(
                    "@total",
                    fact.Total
                );

                command.ExecuteNonQuery();
            }


            // Confirmamos transacción
            transaction.Commit();


            // -------------------------
            // RESUMEN
            // -------------------------

            Console.WriteLine(
                "\nResumen de carga de datos:"
            );

            string[] tables =
            {
                "dim_accounts",
                "dim_customers",
                "account_customers",
                "dim_dates",
                "fact_transactions"
            };


            foreach (
                string table
                in tables
            )
            {
                using var command =
                    connection.CreateCommand();

                command.CommandText =
                    $"SELECT COUNT(*) FROM {table}";

                long count =
                    (long)command.ExecuteScalar();

                Console.WriteLine(
                    $"{table}: {count} filas insertadas"
                );
            }
        }
    }


    // =====================================================
    // MODELOS
    // =====================================================

    public class Account
    {
        public string account_id { get; set; }

        public double limit { get; set; }

        public List<string> products { get; set; }

        public int GeneratedAccountId { get; set; }
    }


    public class Customer
    {
        public string name { get; set; }

        public string username { get; set; }

        public string birthdate { get; set; }

        public List<string> accounts { get; set; }
    }


    public class TransactionGroup
    {
        public string account_id { get; set; }

        public int transaction_count { get; set; }

        public List<TransactionData> transactions { get; set; }
    }


    public class TransactionData
    {
        public string date { get; set; }

        public double amount { get; set; }

        public string transaction_code { get; set; }

        public string symbol { get; set; }

        public double price { get; set; }

        public double total { get; set; }
    }


    // =====================================================
    // DIMENSIONES
    // =====================================================

    public class DimAccount
    {
        public int AccountId { get; set; }

        public double LimitAmount { get; set; }

        public string Products { get; set; }
    }


    public class DimCustomer
    {
        public int CustomerId { get; set; }

        public string Name { get; set; }

        public string Username { get; set; }

        public string BirthDate { get; set; }

        public List<string> Accounts { get; set; }
    }


    public class AccountCustomer
    {
        public int CustomerId { get; set; }

        public int AccountId { get; set; }
    }


    public class DimDate
    {
        public int DateId { get; set; }

        public string Date { get; set; }
    }


    public class FactTransaction
    {
        public int TransactionId { get; set; }

        public int AccountId { get; set; }

        public int DateId { get; set; }

        public int TransactionCount { get; set; }

        public double Amount { get; set; }

        public string TransactionType { get; set; }

        public string Symbol { get; set; }

        public double Price { get; set; }

        public double Total { get; set; }
    }


    // =====================================================
    // RESULTADO DE TRANSFORM
    // =====================================================

    public class TransformResult
    {
        public Dictionary<int, DimAccount>
            DimAccounts { get; set; }

        public Dictionary<int, DimCustomer>
            DimCustomers { get; set; }

        public List<AccountCustomer>
            AccountCustomerMapping { get; set; }

        public List<DimDate>
            DimDates { get; set; }

        public List<FactTransaction>
            FactTransactions { get; set; }
    }
}
