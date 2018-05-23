using System.Collections.Generic;
using System.Data;

namespace Kesco.Lib.DALC
{
    /// <summary>
    ///     Класс для работы с Sql-командами
    /// </summary>
    public class DBCommand
    {

        /// <summary>
        /// Цель, назначение или описание команды
        /// </summary>
        public string Appointment { get; set; }

        /// <summary>
        /// Код сущности
        /// </summary>
        public string EntityId { get; set; }

        /// <summary>
        ///     Словарь входящих параметров
        /// </summary>
        public Dictionary<string, object> ParamsIn;

        /// <summary>
        ///     Словарь исходящих параметров
        /// </summary>
        public Dictionary<string, object> ParamsOut;

        /// <summary>
        ///     Строка подключения
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        ///     Текст запроса
        /// </summary>
        public string Text { get; set; }

        /// <summary>
        ///     Тип выполняемого запроса;
        /// </summary>
        public CommandType Type { get; set; }
    }
}