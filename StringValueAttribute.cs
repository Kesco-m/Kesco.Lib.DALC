using System;

namespace Kesco.Lib.DALC
{
    /// <summary>
    /// Класс, реализующий аттрибут, которым помечается значение в Enum, для последующего получения текствого описания значения Enum
    /// </summary>
    public class StringValueAttribute : Attribute
    {
        /// <summary>
        /// Значение аттрибута
        /// </summary>
        private readonly string _value;

        /// <summary>
        /// Метод установки значения
        /// </summary>
        /// <param name="value">Значение</param>
        public StringValueAttribute(string value)
        {
            _value = value;
        }

        /// <summary>
        /// Значение аттрибута
        /// </summary>
        public string Value
        {
            get { return _value; }
        }
    }


}
