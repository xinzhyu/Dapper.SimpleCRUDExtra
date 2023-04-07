using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using static Dapper.SimpleCRUD;

namespace Dapper
{

    /// <summary>
    /// Database server dialects
    /// </summary>
    public enum Dialect
    {
        SQLServer,
        SQLServer12,
        //PostgreSQL,
        //SQLite,
        MySQL,
        //Oracle,
        //DB2
    }

    public struct DBTypeInfo
    {
        public Dialect Dialect { get; set; }
        public string Encapsulation { get; set; }
        public string GetIdentitySql { get; set; }
        public string GetPagedListSql { get; set; }
    }

    public class ExtraCRUD
    {
        private static readonly ConcurrentDictionary<Type, string> TableNames = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<string, string> ColumnNames = new ConcurrentDictionary<string, string>();
        private static readonly ConcurrentDictionary<Dialect, DBTypeInfo> DBTypeInfos = new ConcurrentDictionary<Dialect, DBTypeInfo>();

        private readonly Dialect _dialect;
        private static readonly ConcurrentDictionary<string, string> StringBuilderCacheDict = new ConcurrentDictionary<string, string>();
        private static bool StringBuilderCacheEnabled = true;

        private ITableNameResolver _tableNameResolver = new TableNameResolver();
        private IColumnNameResolver _columnNameResolver = new ColumnNameResolver();

        /// <summary>
        /// Returns the current dialect name
        /// </summary>
        /// <returns></returns>
        public static DBTypeInfo GetDialect(Dialect dialect)
        {
            return DBTypeInfos[dialect];
        }

        static ExtraCRUD()
        {
            DBTypeInfos.TryAdd(Dialect.SQLServer, new DBTypeInfo
            {
                Dialect = Dialect.SQLServer,
                Encapsulation = "[{0}]",
                GetIdentitySql = string.Format("SELECT CAST(SCOPE_IDENTITY()  AS BIGINT) AS [id]"),
                GetPagedListSql = "SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY {OrderBy}) AS PagedNumber, {SelectColumns} FROM {TableName} {WhereClause}) AS u WHERE PagedNumber BETWEEN (({PageNumber}-1) * {RowsPerPage} + 1) AND ({PageNumber} * {RowsPerPage})"
            });

            DBTypeInfos.TryAdd(Dialect.SQLServer12, new DBTypeInfo
            {
                Dialect = Dialect.SQLServer12,
                Encapsulation = "[{0}]",
                GetIdentitySql = string.Format("SELECT CAST(SCOPE_IDENTITY()  AS BIGINT) AS [id]"),
                GetPagedListSql = "SELECT * FROM {TableName} {WhereClause} ORDER BY {OrderBy} OFFSET (({PageNumber}-1) * {RowsPerPage}) ROWS FETCH NEXT {RowsPerPage} ROWS ONLY"
            });

            DBTypeInfos.TryAdd(Dialect.MySQL, new DBTypeInfo
            {
                Dialect = Dialect.MySQL,
                Encapsulation = "`{0}`",
                GetIdentitySql = string.Format("SELECT LAST_INSERT_ID() AS id"),
                GetPagedListSql = "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {Offset},{RowsPerPage}"
            });
        }

        public ExtraCRUD(Dialect dialect)
        {
            _dialect = dialect;
        }




        //Gets the table name for this entity
        //For Inserts and updates we have a whole entity so this method is used
        //Uses class name by default and overrides if the class has a Table attribute
        public string GetTableName(object entity)
        {
            var type = entity.GetType();
            return GetTableName(type);
        }

        //Gets the table name for this type
        //For Get(id) and Delete(id) we don't have an entity, just the type so this method is used
        //Use dynamic type to be able to handle both our Table-attribute and the DataAnnotation
        //Uses class name by default and overrides if the class has a Table attribute
        public string GetTableName(Type type)
        {
            string tableName;

            if (TableNames.TryGetValue(type, out tableName))
                return tableName;

            tableName = _tableNameResolver.ResolveTableName(type, this);

            TableNames.AddOrUpdate(type, tableName, (t, v) => tableName);

            return tableName;
        }



        public string GetColumnName(PropertyInfo propertyInfo)
        {
            string columnName, key = string.Format("{0}.{1}", propertyInfo.DeclaringType, propertyInfo.Name);

            if (ColumnNames.TryGetValue(key, out columnName))
                return columnName;

            columnName = _columnNameResolver.ResolveColumnName(propertyInfo, this);

            ColumnNames.AddOrUpdate(key, columnName, (t, v) => columnName);

            return columnName;
        }



        public void BuildWhere<TEntity>(StringBuilder sb, IEnumerable<PropertyInfo> idProps, object whereConditions = null)
        {
            var propertyInfos = idProps.ToArray();
            for (var i = 0; i < propertyInfos.Count(); i++)
            {
                var useIsNull = false;

                //match up generic properties to source entity properties to allow fetching of the column attribute
                //the anonymous object used for search doesn't have the custom attributes attached to them so this allows us to build the correct where clause
                //by converting the model type to the database column name via the column attribute
                var propertyToUse = propertyInfos.ElementAt(i);
                var sourceProperties = GetScaffoldableProperties<TEntity>().ToArray();
                for (var x = 0; x < sourceProperties.Count(); x++)
                {
                    if (sourceProperties.ElementAt(x).Name == propertyToUse.Name)
                    {
                        if (whereConditions != null && propertyToUse.CanRead && (propertyToUse.GetValue(whereConditions, null) == null || propertyToUse.GetValue(whereConditions, null) == DBNull.Value))
                        {
                            useIsNull = true;
                        }
                        propertyToUse = sourceProperties.ElementAt(x);
                        break;
                    }
                }
                sb.AppendFormat(
                    useIsNull ? "{0} is null" : "{0} = @{1}",
                    GetColumnName(propertyToUse),
                    propertyToUse.Name);

                if (i < propertyInfos.Count() - 1)
                    sb.AppendFormat(" and ");
            }
        }

        //build insert values which include all properties in the class that are:
        //Not named Id
        //Not marked with the Editable(false) attribute
        //Not marked with the [Key] attribute (without required attribute)
        //Not marked with [IgnoreInsert]
        //Not marked with [NotMapped]
        public void BuildInsertValues<T>(StringBuilder masterSb)
        {
            StringBuilderCache(masterSb, $"{typeof(T).FullName}_BuildInsertValues", sb =>
            {

                var props = GetScaffoldableProperties<T>().ToArray();
                for (var i = 0; i < props.Count(); i++)
                {
                    var property = props.ElementAt(i);
                    if (property.PropertyType != typeof(Guid) && property.PropertyType != typeof(string)
                          && property.GetCustomAttributes(true).Any(attr => attr.GetType().Name == typeof(KeyAttribute).Name)
                          && property.GetCustomAttributes(true).All(attr => attr.GetType().Name != typeof(RequiredAttribute).Name))
                        continue;
                    if (property.GetCustomAttributes(true).Any(attr =>
                        attr.GetType().Name == typeof(IgnoreInsertAttribute).Name ||
                        attr.GetType().Name == typeof(NotMappedAttribute).Name ||
                        attr.GetType().Name == typeof(ReadOnlyAttribute).Name && IsReadOnly(property))
                    ) continue;

                    if (property.Name.Equals("Id", StringComparison.OrdinalIgnoreCase) && property.GetCustomAttributes(true).All(attr => attr.GetType().Name != typeof(RequiredAttribute).Name) && property.PropertyType != typeof(Guid)) continue;

                    sb.AppendFormat("@{0}", property.Name);
                    if (i < props.Count() - 1)
                        sb.Append(", ");
                }
                if (sb.ToString().EndsWith(", "))
                    sb.Remove(sb.Length - 2, 2);
            });
        }

        //build insert parameters which include all properties in the class that are not:
        //marked with the Editable(false) attribute
        //marked with the [Key] attribute
        //marked with [IgnoreInsert]
        //named Id
        //marked with [NotMapped]
        public void BuildInsertParameters<T>(StringBuilder masterSb)
        {
            StringBuilderCache(masterSb, $"{typeof(T).FullName}_BuildInsertParameters", sb =>
            {
                var props = GetScaffoldableProperties<T>().ToArray();

                for (var i = 0; i < props.Count(); i++)
                {
                    var property = props.ElementAt(i);
                    if (property.PropertyType != typeof(Guid) && property.PropertyType != typeof(string)
                          && property.GetCustomAttributes(true).Any(attr => attr.GetType().Name == typeof(KeyAttribute).Name)
                          && property.GetCustomAttributes(true).All(attr => attr.GetType().Name != typeof(RequiredAttribute).Name))
                        continue;
                    if (property.GetCustomAttributes(true).Any(attr =>
                        attr.GetType().Name == typeof(IgnoreInsertAttribute).Name ||
                        attr.GetType().Name == typeof(NotMappedAttribute).Name ||
                        attr.GetType().Name == typeof(ReadOnlyAttribute).Name && IsReadOnly(property))) continue;

                    if (property.Name.Equals("Id", StringComparison.OrdinalIgnoreCase) && property.GetCustomAttributes(true).All(attr => attr.GetType().Name != typeof(RequiredAttribute).Name) && property.PropertyType != typeof(Guid)) continue;

                    sb.Append(GetColumnName(property));
                    if (i < props.Count() - 1)
                        sb.Append(", ");
                }
                if (sb.ToString().EndsWith(", "))
                    sb.Remove(sb.Length - 2, 2);
            });
        }

        //build update statement based on list on an entity
        public void BuildUpdateSet<T>(T entityToUpdate, StringBuilder masterSb)
        {
            StringBuilderCache(masterSb, $"{typeof(T).FullName}_BuildUpdateSet", sb =>
            {
                var nonIdProps = GetUpdateableProperties(entityToUpdate).ToArray();

                for (var i = 0; i < nonIdProps.Length; i++)
                {
                    var property = nonIdProps[i];

                    sb.AppendFormat("{0} = @{1}", GetColumnName(property), property.Name);
                    if (i < nonIdProps.Length - 1)
                        sb.AppendFormat(", ");
                }
            });
        }

        /// <summary>
        /// Append a Cached version of a strinbBuilderAction result based on a cacheKey
        /// </summary>
        /// <param name="sb"></param>
        /// <param name="cacheKey"></param>
        /// <param name="stringBuilderAction"></param>
        public void StringBuilderCache(StringBuilder sb, string cacheKey, Action<StringBuilder> stringBuilderAction)
        {
            if (StringBuilderCacheEnabled && StringBuilderCacheDict.TryGetValue(cacheKey, out string value))
            {
                sb.Append(value);
                return;
            }

            StringBuilder newSb = new StringBuilder();
            stringBuilderAction(newSb);
            value = newSb.ToString();
            StringBuilderCacheDict.AddOrUpdate(cacheKey, value, (t, v) => value);
            sb.Append(value);
        }


        //build select clause based on list of properties skipping ones with the IgnoreSelect and NotMapped attribute
        public void BuildSelect(StringBuilder masterSb, IEnumerable<PropertyInfo> props)
        {
            StringBuilderCache(masterSb, $"{props.CacheKey()}_BuildSelect", sb =>
            {
                var propertyInfos = props as IList<PropertyInfo> ?? props.ToList();
                var addedAny = false;
                for (var i = 0; i < propertyInfos.Count(); i++)
                {
                    var property = propertyInfos.ElementAt(i);

                    if (property.GetCustomAttributes(true).Any(attr => attr.GetType().Name == typeof(IgnoreSelectAttribute).Name || attr.GetType().Name == typeof(NotMappedAttribute).Name)) continue;

                    if (addedAny)
                        sb.Append(",");
                    sb.Append(GetColumnName(property));
                    //if there is a custom column name add an "as customcolumnname" to the item so it maps properly
                    if (property.GetCustomAttributes(true).SingleOrDefault(attr => attr.GetType().Name == typeof(ColumnAttribute).Name) != null)
                        sb.Append(" as " + Encapsulate(property.Name));
                    addedAny = true;
                }
            });
        }

        public string Encapsulate(string databaseword)
        {
            return string.Format(DBTypeInfos[_dialect].Encapsulation, databaseword);
        }

        /// <summary>
        /// Returns the current dialect 
        /// </summary>
        /// <returns></returns>
        public DBTypeInfo GetDialect()
        {
            return DBTypeInfos[_dialect];
        }


        #region MyRegion
        //public static async Task<int> InsertPartAsync<TEntity>(this IDbConnection connection, TEntity entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null)
        //{
        //    //获取表名
        //    var type = typeof(TEntity);
        //    TableNameResolver tableNameResolver = new TableNameResolver();
        //    string tableName = tableNameResolver.ResolveTableName(type);

        //    StringBuilder stringBuilder = new StringBuilder();
        //    stringBuilder.AppendFormat("insert into {0}", tableName);

        //    PropertyInfo[] array = GetUpdateableProperties(entityToInsert).ToArray();
        //    List<string> names = new List<string>();
        //    List<string> values = new List<string>();
        //    int i = 0;
        //    foreach (PropertyInfo property in array)
        //    {
        //        if (IsNullOrDefault(property.GetValue(entityToInsert)))
        //        {
        //            continue;
        //        }

        //        names.Add(property.Name);
        //        values.Add("@"+property.Name);
        //    }

        //    stringBuilder.Append(" (");

        //    stringBuilder.Append(string.Join(",", names));

        //    stringBuilder.Append(") ");

        //    stringBuilder.Append("values");
        //    stringBuilder.Append(" (");

        //    stringBuilder.Append(string.Join(",", values));

        //    stringBuilder.Append(" ) ");

        //    stringBuilder.Append(";");

        //    //if (connection.)
        //    //{
        //    //    stringBuilder.Append("SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS[id]");

        //    //}


        //    var rst = await connection.QueryFirstAsync(stringBuilder.ToString(), entityToInsert, transaction, commandTimeout);
        //    return (int)rst;

        //    //SELECT LAST_INSERT_ID() AS id


        //}

        ///// <summary>
        ///// 简化版修改部分实体(不支持全部功能)
        ///// 复杂功能请使用原版
        ///// </summary>
        ///// <typeparam name="TEntity"></typeparam>
        ///// <param name="connection"></param>
        ///// <param name="entityToUpdate"></param>
        ///// <param name="transaction"></param>
        ///// <param name="commandTimeout"></param>
        ///// <returns></returns>
        //public static async Task<int> UpdatePartAsync<TEntity>(this IDbConnection connection, TEntity entityToUpdate, IDbTransaction transaction = null, int? commandTimeout = null)
        //{
        //    //获取表名
        //    var type = typeof(TEntity);
        //    TableNameResolver tableNameResolver = new TableNameResolver();
        //    string tableName = tableNameResolver.ResolveTableName(type);

        //    StringBuilder sb = new StringBuilder();
        //    sb.AppendFormat("update {0}", tableName);
        //    sb.AppendFormat(" set ");

        //    PropertyInfo[] array = GetUpdateableProperties(entityToUpdate).ToArray();
        //    for (int i = 0; i < array.Length; i++)
        //    {
        //        PropertyInfo propertyInfo = array[i];
        //        if (IsNullOrDefault(propertyInfo.GetValue(entityToUpdate)))
        //        {
        //            continue;
        //        }

        //        if (i != 0)
        //        {
        //            sb.Append(",");
        //        }

        //        sb.AppendFormat("{0} = @{0}", propertyInfo.Name);

        //    }


        //    sb.Append(" where ");

        //    List<PropertyInfo> list = GetIdProperties(type).ToList();

        //    for (int i = 0; i < list.Count; i++)
        //    {
        //        sb.AppendFormat("{0} = @{0}", list[i].Name);
        //        if (i < list.Count - 1)
        //        {
        //            sb.AppendFormat(" and ");
        //        }
        //    }


        //    return await connection.ExecuteAsync(sb.ToString(), entityToUpdate, transaction, commandTimeout);
        //}


        //public static bool IsNullOrDefault(object argument)
        //{
        //    if (argument == null)
        //    {
        //        return true;
        //    }

        //    if (argument is DateTime)
        //    {
        //        return (DateTime)argument == DateTime.MinValue;
        //    }

        //    return false;
        //}

        //private static IEnumerable<PropertyInfo> GetIdProperties(Type type)
        //{
        //    List<PropertyInfo> list = (from p in type.GetProperties()
        //                               where p.GetCustomAttributes(inherit: true).Any((object attr) => attr.GetType().Name == typeof(KeyAttribute).Name)
        //                               select p).ToList();
        //    if (!list.Any())
        //    {
        //        return from p in type.GetProperties()
        //               where p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase)
        //               select p;
        //    }

        //    return list;
        //}

        //private static IEnumerable<PropertyInfo> GetUpdateableProperties<T>(T entity)
        //{
        //    return from p in GetScaffoldableProperties<T>()
        //           where !p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase)
        //           where !p.GetCustomAttributes(inherit: true).Any((object attr) => attr.GetType().Name == typeof(KeyAttribute).Name)
        //           where !p.GetCustomAttributes(inherit: true).Any((object attr) => attr.GetType().Name == typeof(ReadOnlyAttribute).Name)
        //           where !p.GetCustomAttributes(inherit: true).Any((object attr) => attr.GetType().Name == typeof(IgnoreUpdateAttribute).Name)
        //           where !p.GetCustomAttributes(inherit: true).Any((object attr) => attr.GetType().Name == typeof(NotMappedAttribute).Name)
        //           select p;
        //}

        //private static IEnumerable<PropertyInfo> GetScaffoldableProperties<T>()
        //{
        //    return from p in typeof(T).GetProperties() select p;
        //}

        #endregion




    }
}
