using System;

namespace Orleans.Persistence.MSSQLDapper
{
    /// <summary>
    /// MSSQL Dapper grain storage options.
    /// </summary>
    public class MSSQLStorageOptions
    {
        /// <summary>
        /// Connection string for MSSQL storage.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Stage of silo lifecycle where storage should be initialized.  Storage must be initialzed prior to use.
        /// </summary>
        public int InitStage { get; set; } = ServiceLifecycleStage.ApplicationServices;
    }
}
