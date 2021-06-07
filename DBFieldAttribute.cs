using System;

namespace Kesco.Lib.DALC
{
    /// <summary>
    ///     Класс, реализующий возможность отметки поля класса как поля в таблице БД
    /// </summary>
    public class DBFieldAttribute : Attribute
    {
        /// <summary>
        ///     Данный конструктор использовать только для инициализации поля первичного ключа
        /// </summary>
        /// <param name="fieldName">Название поля первичного ключа</param>
        /// <param name="defaultValue">Значение по-умолчанию</param>
        public DBFieldAttribute(string fieldName, object defaultValue)
        {
            FieldName = fieldName;
            IsPK = true;
            IsUpdateble = false;
            IsLinkParent = false;
            ParamName = "";
            DefaultValue = defaultValue;
        }

        /// <summary>
        ///     Конструктор для любого поля, кроме первичного ключа
        /// </summary>
        /// <param name="fieldName">Название поля БД</param>
        /// <param name="paramName">Название параметра в sql-запросе</param>
        /// <param name="isUpdateble">Поле является обновляемым</param>
        /// <param name="isLinkParent">Ссылка на родительскую таблицу</param>
        public DBFieldAttribute(string fieldName, string paramName = "", bool isUpdateble = true,
            bool isLinkParent = false)
        {
            FieldName = fieldName;
            IsPK = false;
            IsUpdateble = isUpdateble;
            IsLinkParent = isLinkParent;
            ParamName = paramName;
            DefaultValue = null;
        }

        /// <summary>
        ///     Название поля БД
        /// </summary>
        public string FieldName { get; set; }

        /// <summary>
        ///     Название поля БД
        /// </summary>
        public string ParamName { get; set; }

        /// <summary>
        ///     Поле является первичным ключом
        /// </summary>
        public bool IsPK { get; set; }

        /// <summary>
        ///     Поле является cсылку на родительскую таблицу
        /// </summary>
        public bool IsLinkParent { get; set; }

        /// <summary>
        ///     Поле является обновляемым
        /// </summary>
        public bool IsUpdateble { get; set; }

        /// <summary>
        ///     Порядковый номер колонки в запросе
        /// </summary>
        public int Ordinal { get; set; }

        /// <summary>
        ///     Значение по-умолчанию
        /// </summary>
        public object DefaultValue { get; set; }
    }

    /// <summary>
    ///     Класс, реализующий возможность отметки поля Binder как поля в таблице БД
    /// </summary>
    public class BinderDBFieldAttribute : DBFieldAttribute
    {
        /// <summary>
        ///     Конструктор для любого поля, кроме первичного ключа
        /// </summary>
        /// <param name="fieldName">Название поля БД</param>
        public BinderDBFieldAttribute(string fieldName) 
            : base(fieldName)
        {
        }
    }
}