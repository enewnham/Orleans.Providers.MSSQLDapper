using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Storage;

namespace Orleans.Persistence.MSSQLDapper
{
    /// <summary>
    /// Factory used to create instances of MSSQL Dapper grain storage.
    /// </summary>
    public static class MSSQLStorageFactory
    {
        /// <summary>
        /// Creates a grain storage instance.
        /// </summary>
        public static IGrainStorage Create(IServiceProvider serviceProvider, string name)
        {
            var options = serviceProvider.GetRequiredService<IOptionsMonitor<MSSQLStorageOptions>>();
            return ActivatorUtilities.CreateInstance<MSSQLGrainStorage>(serviceProvider, options.Get(name), name);
        }
    }
}