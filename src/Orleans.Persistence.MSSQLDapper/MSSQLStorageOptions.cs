using System;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Persistence.MSSQLDapper
{
    /// <summary>
    /// MSSQL Dapper grain storage options.
    /// </summary>
    public class MSSQLStorageOptions : IStorageProviderSerializerOptions
    {
        /// <summary>
        /// Connection string for MSSQL storage.
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Add ApplicationIntent=ReadOnly to the connection string
        /// </summary>
        public bool UseReadOnlyIntent { get; set; }

        /// <summary>
        /// Stage of silo lifecycle where storage should be initialized.  Storage must be initialzed prior to use.
        /// </summary>
        public int InitStage { get; set; } = ServiceLifecycleStage.ApplicationServices;

        internal string ReadOnlyIntent => UseReadOnlyIntent ? ";ApplicationIntent=ReadOnly" : "";

        public IGrainStorageSerializer GrainStorageSerializer { get; set; }
    }

    /// <summary>
    /// ConfigurationValidator for MSSQLDapperGrainStorageOptions
    /// </summary>
    public class MSSQLDapperGrainStorageOptionsValidator : IConfigurationValidator
    {
        private readonly MSSQLStorageOptions options;
        private readonly string name;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="configurationOptions">The option to be validated.</param>
        /// <param name="name">The name of the option to be validated.</param>
        public MSSQLDapperGrainStorageOptionsValidator(MSSQLStorageOptions configurationOptions, string name)
        {
            this.options = configurationOptions ?? throw new OrleansConfigurationException($"Invalid MSSqlDaperGrainStorageOptions for MSSqlDaperGrainStorage {name}. Options is required.");
            this.name = name;
        }

        /// <inheritdoc cref="IConfigurationValidator"/>
        public void ValidateConfiguration()
        {
            if (string.IsNullOrWhiteSpace(this.options.ConnectionString))
            {
                throw new OrleansConfigurationException($"Invalid {nameof(MSSQLStorageOptions)} values for {nameof(MSSQLGrainStorage)} \"{name}\". {nameof(options.ConnectionString)} is required.");
            }
        }
    }
}
