using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Dapper
{
    public partial class SimpleCRUD
    {
        public static async Task<int> InsertPartAsync<TEntity>(this IDbConnection connection, TEntity entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null, Dialect dialect = Dialect.SQLServer)
        {
            ExtraCRUD extraCRUD = dialect.GetExtraCRUD();
            var dbInfo = extraCRUD.GetDialect();

            //获取表名
            var type = typeof(TEntity);
            TableNameResolver tableNameResolver = new TableNameResolver();
            string tableName = tableNameResolver.ResolveTableName(type, extraCRUD);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendFormat("insert into {0}", tableName);

            PropertyInfo[] array = GetUpdateableProperties(entityToInsert).ToArray();
            List<string> names = new List<string>();
            List<string> values = new List<string>();
            foreach (PropertyInfo property in array)
            {
                if (IsNullOrDefault(property.GetValue(entityToInsert)))
                {
                    continue;
                }

                names.Add(property.Name);
                values.Add("@" + property.Name);
            }

            stringBuilder.Append(" (");
            stringBuilder.Append(string.Join(",", names));
            stringBuilder.Append(") ");
            stringBuilder.Append("values");
            stringBuilder.Append(" (");
            stringBuilder.Append(string.Join(",", values));
            stringBuilder.Append(" ) ");
            stringBuilder.Append(";");

            stringBuilder.Append(dbInfo.GetIdentitySql);

            var rst = await connection.QueryFirstAsync(stringBuilder.ToString(), entityToInsert, transaction, commandTimeout);
            return (int)rst;
        }

        /// <summary>
        /// 简化版修改部分实体(不支持全部功能)
        /// 复杂功能请使用原版
        /// </summary>
        /// <typeparam name="TEntity"></typeparam>
        /// <param name="connection"></param>
        /// <param name="entityToUpdate"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        public static async Task<int> UpdatePartAsync<TEntity>(this IDbConnection connection, TEntity entityToUpdate, IDbTransaction transaction = null, int? commandTimeout = null, Dialect dialect = Dialect.SQLServer)
        {
            ExtraCRUD extraCRUD = dialect.GetExtraCRUD();
            var dbInfo = extraCRUD.GetDialect();

            //获取表名
            var type = typeof(TEntity);
            TableNameResolver tableNameResolver = new TableNameResolver();
            string tableName = tableNameResolver.ResolveTableName(type, extraCRUD);

            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("update {0}", tableName);
            sb.AppendFormat(" set ");

            PropertyInfo[] array = GetUpdateableProperties(entityToUpdate).ToArray();
            for (int i = 0; i < array.Length; i++)
            {
                PropertyInfo propertyInfo = array[i];
                if (IsNullOrDefault(propertyInfo.GetValue(entityToUpdate)))
                {
                    continue;
                }

                if (i != 0)
                {
                    sb.Append(",");
                }

                sb.AppendFormat("{0} = @{0}", propertyInfo.Name);

            }


            sb.Append(" where ");

            List<PropertyInfo> list = GetIdProperties(type).ToList();

            for (int i = 0; i < list.Count; i++)
            {
                sb.AppendFormat("{0} = @{0}", list[i].Name);
                if (i < list.Count - 1)
                {
                    sb.AppendFormat(" and ");
                }
            }


            return await connection.ExecuteAsync(sb.ToString(), entityToUpdate, transaction, commandTimeout);
        }



        public static bool IsNullOrDefault(object argument)
        {
            if (argument == null)
            {
                return true;
            }

            if (argument is DateTime)
            {
                return (DateTime)argument == DateTime.MinValue;
            }

            return false;
        }

    }
}
