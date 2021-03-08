using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Persistence.MSSQLDapper;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;

namespace Microsoft.Extensions.Hosting
{
    /// <summary>
    /// Hosting extensions for the MSSQL Dapper storage provider.
    /// </summary>
    public static class MSSQLPersistenceHostingExtensions
    {
        /// <summary>
        /// Adds a MSSQLDapper grain storage provider as the default provider
        /// </summary>
        public static ISiloHostBuilder AddMSSQLDapperGrainStorageAsDefault(this ISiloHostBuilder builder, Action<MSSQLStorageOptions> configureOptions)
        {
            return builder.AddMSSQLDapperGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Adds a MSSQLDapper grain storage provider.
        /// </summary>
        public static ISiloHostBuilder AddMSSQLDapperGrainStorage(this ISiloHostBuilder builder, string name, Action<MSSQLStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddMSSQLDapperGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Adds a MSSQLDapper grain storage provider as the default provider
        /// </summary>
        public static ISiloHostBuilder AddMSSQLDapperGrainStorageAsDefault(this ISiloHostBuilder builder, Action<OptionsBuilder<MSSQLStorageOptions>> configureOptions = null)
        {
            return builder.AddMSSQLDapperGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Adds a MSSQLDapper grain storage provider.
        /// </summary>
        public static ISiloHostBuilder AddMSSQLDapperGrainStorage(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<MSSQLStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddMSSQLDapperGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Adds a MSSQLDapper grain storage provider as the default provider
        /// </summary>
        public static ISiloBuilder AddMSSQLDapperGrainStorageAsDefault(this ISiloBuilder builder, Action<MSSQLStorageOptions> configureOptions)
        {
            return builder.AddMSSQLDapperGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Adds a MSSQLDapper grain storage provider.
        /// </summary>
        public static ISiloBuilder AddMSSQLDapperGrainStorage(this ISiloBuilder builder, string name, Action<MSSQLStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddMSSQLDapperGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Adds a MSSQLDapper grain storage provider as the default provider
        /// </summary>
        public static ISiloBuilder AddMSSQLDapperGrainStorageAsDefault(this ISiloBuilder builder, Action<OptionsBuilder<MSSQLStorageOptions>> configureOptions = null)
        {
            return builder.AddMSSQLDapperGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Adds a MSSQLDapper grain storage provider.
        /// </summary>
        public static ISiloBuilder AddMSSQLDapperGrainStorage(this ISiloBuilder builder, string name, Action<OptionsBuilder<MSSQLStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddMSSQLDapperGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Adds a MSSQLDapper grain storage provider as the default provider
        /// </summary>
        public static IServiceCollection AddMSSQLDapperGrainStorageAsDefault(this IServiceCollection services, Action<MSSQLStorageOptions> configureOptions)
        {
            return services.AddMSSQLDapperGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Adds a MSSQLDapper grain storage provider.
        /// </summary>
        public static IServiceCollection AddMSSQLDapperGrainStorage(this IServiceCollection services, string name, Action<MSSQLStorageOptions> configureOptions)
        {
            return services.AddMSSQLDapperGrainStorage(name, ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Adds a MSSQLDapper grain storage provider as the default provider
        /// </summary>
        public static IServiceCollection AddMSSQLDapperGrainStorageAsDefault(this IServiceCollection services, Action<OptionsBuilder<MSSQLStorageOptions>> configureOptions = null)
        {
            return services.AddMSSQLDapperGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Adds a MSSQLDapper grain storage provider.
        /// </summary>
        public static IServiceCollection AddMSSQLDapperGrainStorage(this IServiceCollection services, string name,
            Action<OptionsBuilder<MSSQLStorageOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<MSSQLStorageOptions>(name));
            // services.AddTransient<IConfigurationValidator>(sp => new MSSQLStorageOptionsValidator(sp.GetService<IOptionsMonitor<MSSQLStorageOptions>>().Get(name), name));
            services.ConfigureNamedOptionForLogging<MSSQLStorageOptions>(name);
            services.TryAddSingleton(sp => sp.GetServiceByName<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            return services.AddSingletonNamedService(name, MSSQLStorageFactory.Create)
                           .AddSingletonNamedService(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<IGrainStorage>(n));
        }
    }
}