using System;

namespace Kesco.Lib.DALC
{
    /// <summary>
    ///     Класс, реализующий возможность указания для класса название источника данных таблицы/представления
    /// </summary>
    public class DBSourceAttribute : Attribute
    {
        /// <summary>
        ///     Конструктор
        /// </summary>
        /// <param name="tableName">Название таблицы/представления, через которую происходит изменение данных</param>
        public DBSourceAttribute(string tableName)
        {
            TableName = tableName;
            RecordSource = "";
            RecordsSource = "";
        }

        /// <summary>
        ///     Конструктор
        /// </summary>
        /// <param name="tableName">Название таблицы/представления, через которую происходит изменение данных</param>
        /// <param name="recordSource">Источник данных для заполнения объекта, если не указан используется tableName</param>
        /// ///
        /// <param name="recordsSource">
        ///     Источник данных для заполнения списка объектов объекта, если не указан используется
        ///     tableName
        /// </param>
        public DBSourceAttribute(string tableName, string recordSource, string recordsSource)
        {
            TableName = tableName;
            RecordSource = recordSource;
            RecordsSource = recordsSource;
        }

        /// <summary>
        ///     Название таблицы/представление в БД
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        ///     Источник данных SUBQUERY их SQLQueries для получения одного объекта
        /// </summary>
        public string RecordSource { get; set; }


        /// <summary>
        ///     Источник данных SUBQUERY их SQLQueries для получения списка объектов
        /// </summary>
        public string RecordsSource { get; set; }
    }
}