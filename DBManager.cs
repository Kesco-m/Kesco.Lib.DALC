using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Kesco.Lib.Log;
using System.Text;

namespace Kesco.Lib.DALC
{
    /// <summary>
    ///     Объект управления подключениями и запросами к БД
    /// </summary>
    public class DBManager
    {
        /// <summary>
        ///     Enum, описывающий типы данных
        /// </summary>
        public enum ParameterTypes
        {
            [StringValue("System.Int32")] Int32 = 1,
            [StringValue("System.Decimal")] Decimal = 2,
            [StringValue("System.String")] String = 3
        }

        /// <summary>
        ///     Получение текстового значения Enum
        /// </summary>
        /// <param name="value">Enum</param>
        /// <returns>Текствого значени Enum</returns>
        public static string GetStringValue(Enum value)
        {
            string output = null;
            var type = value.GetType();

            var fi = type.GetField(value.ToString());
            var attrs =
                fi.GetCustomAttributes(typeof (StringValueAttribute),
                    false) as StringValueAttribute[];
            if (attrs.Length > 0)
            {
                output = attrs[0].Value;
            }

            return output;
        }

        /// <summary>
        ///     Получение значение из DBReader
        /// </summary>
        /// <param name="dbReader">DBReader</param>
        /// <param name="type">Тип значения</param>
        /// <param name="ordinal">Порядок колонки</param>
        /// <returns>Значение</returns>
        public static object GetReaderValue(DBReader dbReader, Type type, int ordinal)
        {
            object value = null;
            Type propertyType = Nullable.GetUnderlyingType(type);

            if (propertyType != null)
                type = propertyType;

            switch (type.FullName)
            {
                case "System.Int32":
                    value = dbReader.IsDBNull(ordinal) ? (int?)null : dbReader.GetInt32(ordinal);
                    break;
                case "System.String":
                    value = dbReader.GetString(ordinal);
                    break;
                case "System.DateTime":
                    value = dbReader.GetDateTime(ordinal);
                    break;
            }
            return value;
        }

        #region Приватные вспомогательные методы

        /// <summary>
        ///     Обработка и добавление в запрос параметров
        /// </summary>
        /// <param name="args">Коллекция параметров</param>
        /// <param name="cmd">Запрос к БД с подключением</param>
        private static void FillQueryArgsCollection(Dictionary<string, object> args, SqlCommand cmd)
        {
            object[] vals = null;
            var _type = "";
            Type type = null;

            foreach (var key in args.Keys)
            {
                if (args[key] != null && args[key].GetType().Equals(typeof (object[])))
                {
                    vals = (object[]) args[key];
                    _type = GetStringValue((ParameterTypes) vals[1]);
                    type = Type.GetType(_type);
                    if (vals[0].ToString().Length == 0 && !type.Equals(typeof (string)))
                        cmd.Parameters.AddWithValue(key, DBNull.Value);
                    else
                    {
                        cmd.Parameters.AddWithValue(key,
                            TypeDescriptor.GetConverter(type).ConvertFromInvariantString(vals[0].ToString()));
                    }
                }
                else
                    cmd.Parameters.AddWithValue(key, args[key] == null ? DBNull.Value : args[key]);
            }
        }

        #endregion

        #region Получение данных из БД

        /// <summary>
        ///     Выполнение переданного sql
        /// </summary>
        /// <param name="sql">Текст запроса, который необходимо выполнить</param>
        /// <param name="cn">Строка подключения</param>
        /// <returns>Возвращает DataTable с результами запроса</returns>
        public static DataTable GetData(string sql, string cn)
        {
            return GetData(sql, cn, CommandType.Text, null);
        }

        /// <summary>
        ///     Выполнение переданного sql
        /// </summary>
        /// <param name="sql">Текст запроса, который необходимо выполнить</param>
        /// <param name="cn">Строка подключения</param>
        /// <param name="ctype">Тип sql-команды</param>
        /// <param name="args">Словарь с параметрами: Название параметра с @; значение параметра</param>
        /// <returns>Возвращает DataTable с результами запроса</returns>
        public static DataTable GetData(string sql, string cn, CommandType ctype, Dictionary<string, object> args)
        {
            var _pageNum = "-1";
            var _itemsPerPage = "-1";
            var _pageCount = "-1";
            var _sRez = "";

            return GetData(sql, cn, ctype, args, null, "", "", null, null, null, ref _pageNum, ref _itemsPerPage,
                ref _pageCount, out _sRez);
        }

        /// <summary>
        ///     Выполнение переданного sql
        /// </summary>
        /// <param name="sql">Текст запроса, который необходимо выполнить</param>
        /// <param name="cn">Строка подключения</param>
        /// <param name="ctype">Тип sql-команды</param>
        /// <param name="args">Словарь с параметрами: Название параметра с @; значение параметра</param>
        /// <param name="localParams">Дополнительные параметры фильтрации, ограничения которых накладываются без учета args</param>
        /// <param name="_sort">Порядок сортировки заданный пользователем</param>
        /// <param name="_defaultSort">Порядок сортировки по-умолчанию</param>
        /// <param name="columnList">Словарь с названием и типом колонок, которые возвращает sql</param>
        /// <param name="groupByList">
        ///     Словарь c колонками, по которым возможна группировка. Значением записи в словаре является
        ///     флаг о необходимости группировки по данному полю
        /// </param>
        /// <param name="sumList">Словарь c колонками, по которым необходимо суммирование значений</param>
        /// <param name="_pageNum">Номер текущей страницы на форме списка</param>
        /// <param name="_itemsPerPage">Количество выводимых записей на страницу</param>
        /// <param name="_pageCount">Общее количество страниц</param>
        /// <param name="_sRez">Количество найденных записей</param>
        /// <returns>Возвращает DataTable с результами запроса</returns>
        public static DataTable GetData(string sql, string cn, CommandType ctype,
            Dictionary<string, object> args, StringCollection localParams,
            string _sort, string _defaultSort,
            Dictionary<string, Type> columnList, Dictionary<string, bool> groupByList,
            Dictionary<string, decimal> sumList,
            ref string _pageNum, ref string _itemsPerPage, ref string _pageCount, out string _sRez)
        {
            var dt = new DataTable("Data");

            #region Обработка переданных локальных параметров фильтрации

            if (localParams != null && localParams.Count > 0)
            {
                var filter = "";
                for (var i = 0; i < localParams.Count; i++)
                {
                    if (i > 0) filter += " AND ";
                    filter += localParams[i];
                }
                filter = @" 
WHERE (" + filter + ")";

                sql = string.Format(sql, filter);
            }

            if ((localParams == null || localParams.Count == 0))
                sql = string.Format(sql, "");

            #endregion

            #region Оборачиваем запрос во внешний SELECT для добавления группировки 

            if (ctype.Equals(CommandType.Text) && columnList != null)
            {
                if (groupByList != null && groupByList.FirstOrDefault(x => x.Value).Value)
                {
                    var groupField = from p in groupByList
                        where p.Value
                        select new {Field = p.Key};

                    var sGroup = @"
SELECT ";

                    foreach (var f in columnList)
                    {
                        if (f.Key.Equals(groupByList.FirstOrDefault(x => x.Key == f.Key && x.Value).Key))
                            sGroup += " " + f.Key + ",";
                        else
                        {
                            if (f.Value.Equals(typeof (String)))
                                sGroup += string.Format(" '' {0},", f.Key);
                            else if (f.Value.Equals(typeof (Decimal)))
                                sGroup += string.Format(" SUM({0}) {0},", f.Key);
                            else
                                sGroup += string.Format(" NULL {0},", f.Key);
                        }
                    }
                    sql = sGroup.Substring(0, sGroup.Length - 1) + string.Format(@" 
FROM 
({0}) TGroup 
GROUP BY ", sql);

                    foreach (var f in groupField)
                        sql += f.Field + ",";

                    sql = sql.Substring(0, sql.Length - 1);
                }
            }

            #endregion

            #region Добавляем сортировку

            if (ctype.Equals(CommandType.Text) && (_sort.Length > 0 || _defaultSort.Length > 0))
                sql += @" 
ORDER BY " + (_sort.Length == 0 ? _defaultSort : _sort);

            #endregion

            var da = new SqlDataAdapter(sql, cn);
            da.SelectCommand.CommandType = ctype;

            #region Заполняем коллекцию переданными параметрами, обязательными для запроса

            if (args != null)
                FillQueryArgsCollection(args, da.SelectCommand);

            #endregion

            if (_pageNum.Length == 0) _pageNum = "1";
            if (_itemsPerPage.Length == 0) _itemsPerPage = "2";

            var pageNum = int.Parse(_pageNum);
            var itemsPerPage = int.Parse(_itemsPerPage);
            var index = (pageNum - 1)*itemsPerPage;


            SqlConnection conn = null;
            SqlDataReader dr = null;
            DataTable dtSchema = null;
            _sRez = "0";
            try
            {
                if (pageNum > 0)
                {
                    #region Если требуется постраничная разбивка данных используем DataReader

                    conn = new SqlConnection(cn);
                    da.SelectCommand.Connection = conn;
                    conn.Open();

                    dr = da.SelectCommand.ExecuteReader(CommandBehavior.CloseConnection);
                    dtSchema = dr.GetSchemaTable();

                    if (dtSchema != null)
                    {
                        foreach (DataRow drow in dtSchema.Rows)
                        {
                            var columnName = Convert.ToString(drow["ColumnName"]);
                            var column = new DataColumn(columnName, (Type) (drow["DataType"]));
                            column.Unique = (bool) drow["IsUnique"];
                            column.AllowDBNull = (bool) drow["AllowDBNull"];
                            column.AutoIncrement = (bool) drow["IsAutoIncrement"];
                            dt.Columns.Add(column);
                        }
                    }

                    var i = 0;
                    var n = 0;

                    List<string> sumListCloneKeys = null;
                    if (sumList != null)
                    {
                        sumListCloneKeys = new List<string>(sumList.Keys);
                        for (var j = 0; j < sumListCloneKeys.Count; j++)
                            sumList[sumListCloneKeys[j]] = 0;
                    }

                    while (dr.Read())
                    {
                        if (i >= index && n < itemsPerPage)
                        {
                            var dataRow = dt.NewRow();
                            for (var j = 0; j < dt.Columns.Count; j++)
                            {
                                dataRow[dt.Columns[j]] = dr[j];
                            }
                            dt.Rows.Add(dataRow);
                            n++;
                        }

                        #region Суммируем значения необходимых полей

                        if (sumList != null)
                        {
                            for (var j = 0; j < sumListCloneKeys.Count; j++)
                                sumList[sumListCloneKeys[j]] +=
                                    Convert.ToDecimal(dr.GetValue(dr.GetOrdinal(sumListCloneKeys[j])));
                        }

                        #endregion

                        i++;
                    }

                    _pageCount = ((int) Math.Ceiling(i/(double) itemsPerPage)).ToString();
                    _pageNum = (pageNum == 0) ? "1" : pageNum.ToString();
                    _itemsPerPage = itemsPerPage.ToString();
                    _sRez = i.ToString();

                    #endregion
                }
                else
                    da.Fill(dt);
            }
            catch (Exception ex)
            {
                var dex = new DetailedException(ex.Message, ex, da.SelectCommand);
                Logger.WriteEx(dex);
                throw dex;
            }
            finally
            {
                if (dr != null) dr.Close();
                if (conn != null && conn.State != ConnectionState.Closed) conn.Close();
            }
            return dt;
        }

        #endregion

        #region Методы SQLCommand обращения к БД

        /// <summary>
        ///     Выполнить SQL инструкцию.
        /// </summary>
        /// <param name="comdText">Текст запроса или название ХП</param>
        /// <param name="type">Тип команыды - текст или хранимая процедура</param>
        /// <param name="cn">Строка соединения</param>
        /// <param name="parameters">Входные значения</param>
        /// <param name="parametersOut">Выходные параметры, для хп автоматически формируется возвращаемое значение(@RETURN_VALUE)</param>
        public static void ExecuteNonQuery(string comdText, CommandType type, string cn,
            Dictionary<string, object> parameters = null, Dictionary<string, object> parametersOut = null)
        {
            SqlConnection connect = null;
            SqlCommand cmd = null;

            try
            {
                using (connect = new SqlConnection(cn))
                {
                    using (cmd = new SqlCommand(comdText, connect))
                    {
                        cmd.CommandType = type;

                        if (parameters != null)
                            foreach (var p in parameters)
                                cmd.Parameters.AddWithValue(p.Key, p.Value);

                        if (parametersOut != null)
                            foreach (var p in parametersOut)
                            {
                                var par = cmd.Parameters.AddWithValue(p.Key, p.Value);

                                par.Direction = ParameterDirection.Output;
                                par.Size = -1;
                            }

                        if (type == CommandType.StoredProcedure)
                        {
                            if (parametersOut == null)
                                parametersOut = new Dictionary<string, object> {{"@RETURN_VALUE", -1}};
                            else
                                parametersOut.Add("@RETURN_VALUE", -1);

                            cmd.Parameters.Add(new SqlParameter
                            {
                                Direction = ParameterDirection.ReturnValue,
                                ParameterName = "@RETURN_VALUE"
                            });
                        }

                        connect.Open();
                        cmd.ExecuteNonQuery();

                        if (parametersOut != null)
                            foreach (var p in parametersOut.Keys.ToList())
                                parametersOut[p] = cmd.Parameters[p].Value;
                    }
                }
            }
            catch (SqlException ex)
            {
                StringBuilder sb = new StringBuilder();

                foreach (SqlError e in ex.Errors)
                {
                    if (e.Number != 3609)//Транзакция завершилась в триггере. Выполнение пакета прервано.
                    {
                        if (sb.Length > 0) sb.Append(Environment.NewLine);

                        if (e.Number == 229 || e.Number == 230)
                        {
                            sb.Append("У Вас нет прав для данной операции!");
                        }
                        else
                            sb.Append(e.Message);
                    }
                }

                DetailedException dex = null;
                if (sb.Length < 1) dex = new DetailedException(ex.Message, ex, cmd);
                else dex = new DetailedException(sb.ToString(), ex, cmd);
                Logger.WriteEx(dex);
                throw dex;
            }
            catch (Exception ex)
            {
                var dex = new DetailedException(ex.Message, ex, cmd);
                Logger.WriteEx(dex);
                throw dex;
            }
            finally
            {
                if (connect != null && connect.State == ConnectionState.Open)
                    connect.Close();
            }
        }

        /// <summary>
        ///     Выполнить SQL инструкцию. Используется, когда необходимо указывать дополнительные параметры для SqlParameter -ов
        /// </summary>
        /// <param name="comdText">Текст запроса или название ХП</param>
        /// <param name="type">Тип команыды - текст или хранимая процедура</param>
        /// <param name="cn">Строка соединения</param>
        /// <param name="parameters">Входные значения в виде SqlParameter</param>
        /// <param name="parametersOut">Выходные параметры, для хп автоматически формируется возвращаемое значение(@RETURN_VALUE)</param>
        public static void ExecuteNonQuery(string comdText, CommandType type, string cn,
            IEnumerable<SqlParameter> parameters = null, Dictionary<string, object> parametersOut = null)
        {
            SqlConnection connect = null;
            SqlCommand cmd = null;

            try
            {
                using (connect = new SqlConnection(cn))
                {
                    using (cmd = new SqlCommand(comdText, connect))
                    {
                        cmd.CommandType = type;


                        if (parameters != null)
                            foreach (var p in parameters)
                                cmd.Parameters.Add(p);

                        if (parametersOut != null)
                            foreach (var p in parametersOut)
                            {
                                var par = cmd.Parameters.AddWithValue(p.Key, p.Value);

                                par.Direction = ParameterDirection.Output;
                                par.Size = -1;
                            }

                        if (type == CommandType.StoredProcedure)
                        {
                            if (parametersOut == null)
                                parametersOut = new Dictionary<string, object> {{"@RETURN_VALUE", -1}};
                            else
                                parametersOut.Add("@RETURN_VALUE", -1);

                            cmd.Parameters.Add(new SqlParameter
                            {
                                Direction = ParameterDirection.ReturnValue,
                                ParameterName = "@RETURN_VALUE"
                            });
                        }

                        connect.Open();
                        cmd.ExecuteNonQuery();

                        if (parametersOut != null)
                            foreach (var p in parametersOut.Keys.ToList())
                                parametersOut[p] = cmd.Parameters[p].Value;
                    }
                }
            }
            catch (SqlException ex)
            {
                StringBuilder sb = new StringBuilder();
                foreach (SqlError e in ex.Errors)
                {
                    if (e.Number != 3609)//Транзакция завершилась в триггере. Выполнение пакета прервано.
                    {
                        if (sb.Length > 0) sb.Append(Environment.NewLine);
                        if (e.Number == 229 || e.Number == 230)
                        {
                            sb.Append("У Вас нет прав для данной операции!");
                        }
                        else
                            sb.Append(e.Message);
                    }
                }

                DetailedException dex = null;
                if (sb.Length < 1) dex = new DetailedException(ex.Message, ex, cmd);
                else dex = new DetailedException(sb.ToString(), ex, cmd);
                Logger.WriteEx(dex);
                throw dex;
            }
            catch (Exception ex)
            {
                var dex = new DetailedException(ex.Message, ex, cmd);
                Logger.WriteEx(dex);
                throw dex;
            }
            finally
            {
                if (connect != null && connect.State == ConnectionState.Open)
                    connect.Close();
            }
        }

        /// <summary>
        ///     Выполнить SQL инструкцию.
        /// </summary>
        /// <param name="comdText">Текст запроса или название ХП</param>
        /// <param name="id">Один единственный параметр @id</param>
        /// <param name="type">Тип команыды - текст или хранимая процедура</param>
        /// <param name="cn">Строка соединения</param>
        /// <param name="parametersOut">Выходные параметры, для хп автоматически формируется возвращаемое значение(@RETURN_VALUE)</param>
        /// <remarks>Перегрузка сделана для работы с одним единственным параметром @id</remarks>
        public static void ExecuteNonQuery(string comdText, int id, CommandType type, string cn,
            Dictionary<string, object> parametersOut = null)
        {
            var sqlParams = new Dictionary<string, object> {{"@id", id}};
            ExecuteNonQuery(comdText, type, cn, sqlParams, parametersOut);
        }

        /// <summary>
        ///     Выполняет запрос и возвращает первый столбец первой строки результирующего набора, возвращаемого запросом.
        ///     Дополнительные столбцы и строки игнорируются.
        /// </summary>
        /// <param name="comdText">Текст запроса или название ХП</param>
        /// <param name="type">Тип команыды - текст или хранимая процедура</param>
        /// <param name="cn">Строка соединения</param>
        /// <param name="parameters">Входные значения</param>
        /// <param name="parametersOut">Выходные параметры, для хп автоматически формируется возвращаемое значение(@RETURN_VALUE)</param>
        public static object ExecuteScalar(string comdText, CommandType type, string cn,
            Dictionary<string, object> parameters = null, Dictionary<string, object> parametersOut = null)
        {
            SqlConnection connect = null;
            SqlCommand cmd = null;
            object result;

            try
            {
                using (connect = new SqlConnection(cn))
                {
                    using (cmd = new SqlCommand(comdText, connect))
                    {
                        cmd.CommandType = type;
                        if (parameters != null)
                            foreach (var p in parameters)
                                cmd.Parameters.AddWithValue(p.Key, p.Value);

                        if (parametersOut != null)
                            foreach (var p in parametersOut)
                            {
                                var par = cmd.Parameters.AddWithValue(p.Key, p.Value);

                                par.Direction = ParameterDirection.Output;
                                par.Size = -1;
                            }

                        if (type == CommandType.StoredProcedure)
                        {
                            if (parametersOut == null)
                                parametersOut = new Dictionary<string, object> {{"@RETURN_VALUE", -1}};
                            else
                                parametersOut.Add("@RETURN_VALUE", -1);

                            cmd.Parameters.Add(new SqlParameter
                            {
                                Direction = ParameterDirection.ReturnValue,
                                ParameterName = "@RETURN_VALUE"
                            });
                        }

                        connect.Open();
                        result = cmd.ExecuteScalar();
                        if (parametersOut != null)
                            foreach (var p in parametersOut.Keys.ToList())
                                parametersOut[p] = cmd.Parameters[p].Value;
                    }
                }
            }
            catch (Exception ex)
            {
                var dex = new DetailedException(ex.Message, ex, cmd);
                Logger.WriteEx(dex);
                throw dex;
            }
            finally
            {
                if (connect != null && connect.State == ConnectionState.Open)
                    connect.Close();
            }

            return result;
        }

        /// <summary>
        ///     Выполняет запрос и возвращает первый столбец первой строки результирующего набора, возвращаемого запросом.
        ///     Дополнительные столбцы и строки игнорируются.
        /// </summary>
        /// <param name="comdText">Текст запроса или название ХП</param>
        /// <param name="id">Один единственный параметр @id</param>
        /// <param name="type">Тип команыды - текст или хранимая процедура</param>
        /// <param name="cn">Строка соединения</param>
        /// <param name="parametersOut">Выходные параметры, для хп автоматически формируется возвращаемое значение(@RETURN_VALUE)</param>
        /// <remarks>Перегрузка сделана для работы с одним единственным параметром @id</remarks>
        public static object ExecuteScalar(string comdText, int id, CommandType type, string cn,
            Dictionary<string, object> parametersOut = null)
        {
            var sqlParams = new Dictionary<string, object> {{"@id", id}};
            return ExecuteScalar(comdText, type, cn, sqlParams, parametersOut);
        }

        private SqlDbType GetDbType(object value)
        {
            var type = value.GetType();

            switch (type.Name)
            {
                case "String":
                    return SqlDbType.VarChar;
                case "Int32":
                    return SqlDbType.Int;
                case "Decimal":
                    return SqlDbType.Money;
                case "DateTime":
                    return SqlDbType.DateTime;
                case "Double":
                    return SqlDbType.Float;
                default:
                    throw new ArgumentException("Не реализовано для типа: " + type.Name);
            }
        }

        #endregion
    }
}