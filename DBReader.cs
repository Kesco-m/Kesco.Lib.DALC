using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using Kesco.Lib.Log;

namespace Kesco.Lib.DALC
{
    /// <summary>
    ///  Класс абстрагирует работу с несколькими классами SqlDataReader и SqlConnection, объединяя их в единое целое.
    /// </summary>
    /// <remarks>
    /// Паттерн проектирования "фасад". Рекомендуется использовать в конструкции "using"
    /// Существует программа, преобразования запроса или ХП в класс сущности и начитки данных:
    /// https://titan.kescom.com:8443/svn/web/GeneratorORM
    /// </remarks>
    /// <example>
    ///  Примеры использования и юнит тесты: Kesco.App.UnitTests.DalcTests.DalcTest
    /// </example>
    public class DBReader : IDataReader
    {
        /// <summary>
        ///  Конструктор DBReader
        /// </summary>
        /// <param name="comdText">Текст запроса или название ХП</param>
        /// <param name="type">Тип команыды - текст или хранимая процедура</param>
        /// <param name="connectionString">Строка подключения</param>
        /// <param name="parameters">Входные значения</param>
        public DBReader(string comdText, CommandType type, string connectionString, Dictionary<string, object> parameters = null)
        {
            _connect = new SqlConnection(connectionString);
            ExecuteReader(comdText, type, parameters);
        }

        /// <summary>
        ///  Конструктор DBReader
        /// </summary>
        /// <remarks>
        ///  Для получения OUTPUT параметров необходимо принудительно закрыть ридер - вызвать метод Close(),
        ///  до закрытия параметры остаются без значения, это происходит по тому что Return и OUTPUT параметры
        ///  формируются по окончанию работы ХП, пока открыт SqlDataReader работа ХП считается не завершенной.
        /// </remarks>
        /// <param name="comdText">Текст запроса или название ХП</param>
        /// <param name="type">Тип команыды - текст или хранимая процедура</param>
        /// <param name="connectionString">Строка подключения</param>
        /// <param name="parameters">Входные значения</param>
        /// <param name="parametersOut">Выходные параметры, для хп автоматически формируется возвращаемое значение(@RETURN_VALUE)</param>
        public DBReader(string comdText, CommandType type, string connectionString, Dictionary<string, object> parameters, Dictionary<string, object> parametersOut)
        {
            _connect = new SqlConnection(connectionString);
            _parametersOut = parametersOut;
            ExecuteReader(comdText, type, parameters, parametersOut);
        }

        /// <summary>
        ///  Конструктор DBReader для часто используемой команды с единственным параметром @id
        /// </summary>
        /// <remarks>
        ///  Имя SQL параметра должно быть строго @id 
        /// </remarks>
        /// <param name="comdText">Текст запроса или название ХП, должны содержать строго один параметр @id в нижнем регистре</param>
        /// <param name="id"> Значение параметра @id типа int</param>
        /// <param name="type">Тип команыды - текст или хранимая процедура</param>
        /// <param name="connectionString">Строка подключения</param>
        public DBReader(string comdText, int id, CommandType type, string connectionString)
        {
            _connect = new SqlConnection(connectionString);

            var sqlParams = new Dictionary<string, object> { { "@id", id } };
            ExecuteReader(comdText, type, sqlParams);
        }

        /// <summary>
        ///  Конструктор DBReader для небольшого количества параметров
        /// </summary>
        /// <param name="comdText">Текст запроса или название ХП, должны содержать строго один параметр @id в нижнем регистре</param>
        /// <param name="type">Тип команыды - текст или хранимая процедура</param>
        /// <param name="connectionString">Строка подключения</param>
        /// <param name="pars">Перечисление параметров</param>
        public DBReader(string comdText, CommandType type, string connectionString, params Tuple<string, object>[] pars)
        {
            _connect = new SqlConnection(connectionString);

            var sqlParams = new Dictionary<string, object>(pars.Length);
            foreach (var p in pars)
                 sqlParams.Add(p.Item1, p.Item2);

            ExecuteReader(comdText, type, sqlParams);
        }

        /// <summary>
        ///  Предоставляет подключение к базе данных SQL Server.
        /// </summary>
        private readonly SqlConnection _connect;

        /// <summary>
        ///  Предоставляет способ чтения потока строк последовательного доступа из базы данных SQL Server
        /// </summary>
        private SqlDataReader _reader;

        /// <summary>
        ///  Представляет инструкцию Transact-SQL или хранимую процедуру, выполняемую над базой данных SQL Server
        /// </summary>
        private SqlCommand _command;

        /// <summary>
        ///  Хранит ссылку на выходные праметры
        /// </summary>
        private readonly Dictionary<string, object> _parametersOut;

        /// <summary>
        /// Выполнить SQL инструкцию и получить DataReader
        /// </summary>
        /// <param name="comdText">Текст запроса или название ХП</param>
        /// <param name="type">Тип команыды - текст или хранимая процедура</param>
        /// <param name="parameters">Входные значения</param>
        private void ExecuteReader(string comdText, CommandType type, Dictionary<string, object> parameters = null)
        {
            try
            {
                using (_command = new SqlCommand(comdText, _connect) { CommandType = type })
                {

                    if (parameters != null)
                        foreach (var p in parameters)
                              _command.Parameters.AddWithValue(p.Key, p.Value ?? DBNull.Value);

                    _connect.Open();
                    _reader = _command.ExecuteReader(CommandBehavior.CloseConnection);
                }
            }
            catch (Exception ex)
            {
                DetailedException dex = new DetailedException(ex.Message, ex, _command);
                Logger.WriteEx(dex);
                Dispose();
                throw dex;
            }
        }


        /// <summary>
        /// Выполнить SQL инструкцию и получить DataReader. Версия с выходными параметрами
        /// </summary>
        /// <param name="comdText">Текст запроса или название ХП</param>
        /// <param name="type">Тип команыды - текст или хранимая процедура</param>
        /// <param name="parameters">Входные значения</param>
        /// <param name="parametersOut">Выходные параметры, для хп автоматически формируется возвращаемое значение(@RETURN_VALUE)</param>
        private void ExecuteReader(string comdText, CommandType type, Dictionary<string, object> parameters, Dictionary<string, object> parametersOut)
        {
            try
            {
                using (_command = new SqlCommand(comdText, _connect) { CommandType = type })
                {

                    if (parameters != null)
                        foreach (var p in parameters)
                            _command.Parameters.AddWithValue(p.Key, p.Value ?? DBNull.Value);

                    if (parametersOut != null)
                        foreach (var p in parametersOut)
                        {
                            var par = _command.Parameters.AddWithValue(p.Key, p.Value);

                            par.Direction = ParameterDirection.Output;
                            par.Size = -1;
                        }

                    if (type == CommandType.StoredProcedure)
                    {
                        if (parametersOut == null)
                            parametersOut = new Dictionary<string, object> { { "@RETURN_VALUE", -1 } };
                        else
                            parametersOut.Add("@RETURN_VALUE", -1);

                        _command.Parameters.Add(new SqlParameter
                        {
                            Direction = ParameterDirection.ReturnValue,
                            ParameterName = "@RETURN_VALUE"
                        });
                    }

                    _connect.Open();
                    _reader = _command.ExecuteReader(CommandBehavior.CloseConnection);
                }
            }
            catch (Exception ex)
            {
                DetailedException dex = new DetailedException(ex.Message, ex, _command);
                Logger.WriteEx(dex);
                Dispose();
                throw dex;
            }
        }

        /// <summary>
        ///  Высвобождение ресурсов используемых в DBReader
        /// </summary>
        private void ReleaseResources()
        {
            try
            {
                if (_reader != null)
                {
                    if (!_reader.IsClosed)
                        _reader.Close();

                    _reader.Dispose();
                }

                if (_connect != null)
                {
                    if (_connect.State != ConnectionState.Closed)
                        _connect.Close();

                    _connect.Dispose();
                }

                if(_command != null)
                    _command.Dispose();
            }
            catch (Exception ex)
            {
                DetailedException dex = new DetailedException("Ошибка высвобождения програмных ресурсов в Kesco.Lib.DALC.DBReader " + ex.Message, ex);
                Logger.WriteEx(dex);
                throw dex;
            }
        }

        /// <summary>
        ///  Освобождает ресурсы, используемые объектом DBReader
        /// </summary>
        public void Dispose()
        {
            ReleaseResources();

            // если высвободили ресурсы вручную или через using, то финализатор не вызываем
            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///  Финализатор для забывчивых программистов
        /// </summary>
        ~DBReader()
        {
            ReleaseResources();

            const string error = "Ошибка разработчика при использовании класса DBReader: \n" +
                                 "не высвобождены ресурсы. Рекомендуется использовать класс DBReader в конструкции \"using\"";
            Logger.WriteEx(new DetailedException(error, new ObjectDisposedException("Не вызван метод Dispose")));
        }


        #region Перопределенные методы интерфейса SqlDataReader

        /// <summary>
        /// Возвращает значение, указывающее, является ли SqlDataReader содержит одну или несколько строк.
        /// </summary>
        public bool HasRows{ get { return _reader.HasRows; }}

        /// <summary>
        ///  Возвращает имя указанного столбца.
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public string GetName(int i)
        {
            return _reader.GetName(i);
        }

        /// <summary>
        /// Возвращает строку, представляющую тип данных указанного столбца
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public string GetDataTypeName(int i)
        {
            return _reader.GetDataTypeName(i);
        }

        /// <summary>
        ///  Возвращает тип данных объекта столбца.
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public Type GetFieldType(int i)
        {
            return _reader.GetFieldType(i);
        }

        /// <summary>
        ///  Возвращает значение указанного столбца в его собственном формате
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public object GetValue(int i)
        {
            return _reader.GetValue(i);
        }

        /// <summary>
        ///  Заполняет массив объектов значениями столбцов текущей строки
        /// </summary>
        /// <param name="values">Массив объектов Object, в который необходимо скопировать столбцы атрибутов.</param>
        public int GetValues(object[] values)
        {
            return _reader.GetValues(values);
        }

        /// <summary>
        ///  Возвращает порядковый номер столбца с заданным именем столбца
        /// </summary>
        /// <param name="name">Имя столбца.</param>
        public int GetOrdinal(string name)
        {
            return _reader.GetOrdinal(name);
        }

        /// <summary>
        ///  Возвращает значение указанного столбца в виде логического значения
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public bool GetBoolean(int i)
        {
            return _reader.GetBoolean(i);
        }

        /// <summary>
        ///  Возвращает значение указанного столбца в виде байта
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public byte GetByte(int i)
        {
            return _reader.GetByte(i);
        }

        /// <summary>
        ///  Считывает поток байтов из указанного смещения столбца в буфер массива, начиная с заданного смещения буфера
        /// </summary>
        /// <param name="i">Порядковый номер столбца (начиная с нуля)</param>
        /// <param name="fieldOffset">Индекс в поле, с которого начинается операция считывания</param>
        /// <param name="buffer">Буфер, в который необходимо считать поток байтов</param>
        /// <param name="bufferoffset">Индекс в buffer, с которого должна начинаться операция записи</param>
        /// <param name="length">Максимальная длина для копирования в буфер</param>
        /// <returns>Фактическое количество считанных байтов</returns>
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return _reader.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        /// <summary>
        ///  Возвращает значение указанного столбца в виде одного символа
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public char GetChar(int i)
        {
            return _reader.GetChar(i);
        }

        /// <summary>
        ///  Считывает поток символов из заданного смещения столбца в буфер как массив, начиная с заданного смещения буфера
        /// </summary>
        /// <param name="i">Порядковый номер столбца (начиная с нуля)</param>
        /// <param name="fieldoffset">Индекс в поле, с которого начинается операция считывания</param>
        /// <param name="buffer">Буфер, в который необходимо считать поток байтов</param>
        /// <param name="bufferoffset">Индекс в buffer, с которого должна начинаться операция записи</param>
        /// <param name="length">Максимальная длина для копирования в буфер</param>
        /// <returns>Фактическое количество считанных байтов</returns>
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return _reader.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        /// <summary>
        ///  Возвращает значение указанного столбца как глобальный уникальный идентификатор (GUID)
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public Guid GetGuid(int i)
        {
            return _reader.GetGuid(i);
        }

        /// <summary>
        ///  Возвращает значение указанного столбца в виде 16-разрядное целое число со знаком
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public short GetInt16(int i)
        {
            return _reader.GetInt16(i);
        }

        /// <summary>
        ///  Возвращает значение указанного столбца в виде 32-разрядное целое число со знаком
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public int GetInt32(int i)
        {
            return _reader.GetInt32(i);
        }

        /// <summary>
        ///  Возвращает значение указанного столбца в виде 64-разрядного целого числа со знаком
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public long GetInt64(int i)
        {
            return _reader.GetInt64(i);
        }

        /// <summary>
        ///  Возвращает значение заданного столбца в виде числа с плавающей запятой одинарной точности
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public float GetFloat(int i)
        {
            return _reader.GetFloat(i);
        }

        /// <summary>
        ///  Возвращает значение заданного столбца в виде числа с плавающей запятой двойной точности
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public double GetDouble(int i)
        {
            return _reader.GetDouble(i);
        }

        /// <summary>
        ///  Возвращает значение указанного столбца в виде строки
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public string GetString(int i)
        {
            return _reader.GetString(i);
        }

        /// <summary>
        ///  Возвращает значение указанного столбца в виде объекта Decimal
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public decimal GetDecimal(int i)
        {
            return _reader.GetDecimal(i);
        }

        /// <summary>
        ///  Возвращает значение указанного столбца в виде объекта DateTime
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public DateTime GetDateTime(int i)
        {
            return _reader.GetDateTime(i);
        }

        /// <summary>
        ///  Возвращает IDataReader для указанного порядкового номера столбца
        /// </summary>
        /// <param name="i">Порядковый номер столбца.</param>
        public IDataReader GetData(int i)
        {
            return _reader.GetData(i);
        }

        /// <summary>
        ///  Возвращает значение, указывающее, содержит ли столбец несуществующие или отсутствующие значения
        /// </summary>
        /// <param name="i">Порядковый номер столбца (от нуля)</param>
        public bool IsDBNull(int i)
        {
            return _reader.IsDBNull(i);
        }

        /// <summary>
        ///  Возвращает число столбцов в текущей строке
        /// </summary>
        public int FieldCount
        {
            get { return _reader.FieldCount; }
        }

        /// <summary>
        ///  Возвращает объект по индексу
        /// </summary>
        /// <param name="i"> индекс объекта</param>
        object IDataRecord.this[int i]
        {
            get { return _reader; }
        }

        /// <summary>
        ///  Возвращает объект по индексу
        /// </summary>
        /// <param name="name"> индекс объекта</param>
        object IDataRecord.this[string name]
        {
            get { return _reader; }
        }

        /// <summary>
        ///  Закрывает подключение к базе данных и объект SqlDataReader
        /// </summary>
        public void Close()
        {

             if (!_reader.IsClosed)
                 _reader.Close();
           

            if (_connect.State != ConnectionState.Closed)
                _connect.Close();


            if (_command != null)
            {
                if (_parametersOut != null)
                    foreach (var p in _parametersOut.Keys.ToList())
                        _parametersOut[p] = _command.Parameters[p].Value;

                _command.Dispose();
            }
        }

        /// <summary>
        ///  Возвращает объект DataTable, описывающий метаданные столбцов модуля чтения данных SqlDataReader
        /// </summary>
        public DataTable GetSchemaTable()
        {
            return _reader.GetSchemaTable();
        }

        /// <summary>
        ///  Перемещает модуль чтения данных к следующему результату при чтении результатов пакетных инструкций Transact-SQL
        /// </summary>
        public bool NextResult()
        {
            return _reader.NextResult();
        }

        /// <summary>
        ///  Перемещает SqlDataReader к следующей записи
        /// </summary>
        public bool Read()
        {
            return _reader.Read();
        }

        /// <summary>
        ///  Возвращает значение, указывающее глубину вложенности для текущей строки
        /// </summary>
        public int Depth { get { return _reader.Depth; }}

        /// <summary>
        ///  Возвращает логическое значение, указывающее, является ли указанный SqlDataReader Закрыть экземпляр
        /// </summary>
        public bool IsClosed { get { return _reader.IsClosed; }}

        /// <summary>
        ///  Возвращает номер строки изменены, вставлены или удалены инструкцией Transact-SQL
        /// </summary>
        public int RecordsAffected { get { return _reader.RecordsAffected; }}

        #endregion
    }
}
