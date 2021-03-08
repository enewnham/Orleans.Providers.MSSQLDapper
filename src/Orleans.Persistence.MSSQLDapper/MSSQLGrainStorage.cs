using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Storage;
using Dapper;
using Microsoft.Data.SqlClient;
using System.Data;
using Orleans.Serialization;
using System;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Diagnostics;
using System.Globalization;

namespace Orleans.Persistence.MSSQLDapper
{
    /// <summary>
    /// MSSQL Dapper grain storage provider.
    /// </summary>
    public class MSSQLGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly string name;
        private readonly ILogger<MSSQLGrainStorage> logger;
        private readonly MSSQLStorageOptions options;
        private readonly SerializationManager serializationManager;
        private readonly IServiceProvider serviceProvider;

        public MSSQLGrainStorage(
            string name,
            MSSQLStorageOptions options,
            SerializationManager serializationManager,
            IServiceProvider serviceProvider,
            ILogger<MSSQLGrainStorage> logger)
        {
            this.name = name;
            this.logger = logger;
            this.options = options;
            this.serializationManager = serializationManager;
            this.serviceProvider = serviceProvider;
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<MSSQLGrainStorage>(this.name), this.options.InitStage, InitAsync, CloseAsync);
        }

        public Task InitAsync(CancellationToken ct)
        {
            var stopWatch = Stopwatch.StartNew();

            try
            {
                var initMsg = string.Format("Init: Name={0}", this.name);

                stopWatch.Stop();
                this.logger.LogInformation((int)ErrorCode.StorageProviderBase,
                    $"Initializing provider {this.name} of type {this.GetType().Name} in stage {this.options.InitStage} took {stopWatch.ElapsedMilliseconds} Milliseconds.");

                return Task.CompletedTask;
            }
            catch (Exception exc)
            {
                stopWatch.Stop();
                this.logger.LogError((int)ErrorCode.Provider_ErrorFromInit, $"Initialization failed for provider {this.name} of type {this.GetType().Name} in stage {this.options.InitStage} in {stopWatch.ElapsedMilliseconds} Milliseconds.", exc);
                throw;
            }
        }

        public Task CloseAsync(CancellationToken ct) => Task.CompletedTask;

        public async Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var grainId = grainReference.ToKeyString();
            var grainStateVersion = ToGrainStateVersion(grainState);
            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.Trace(ErrorCode.StorageProviderBase, $"Clearing grain state: name={this.name} grainType={grainType} grainId={grainId} ETag={grainState.ETag}");
            }

            int? storageVersion = null;
            try
            {
                using var c = new SqlConnection(this.options.ConnectionString);
                storageVersion = await c.QuerySingleOrDefaultAsync<int?>(
                    "ClearStorageKey",
                    param: new
                    {
                        grainId,
                        grainStateVersion
                    },
                    commandType: CommandType.StoredProcedure).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.Error(ErrorCode.StorageProvider_DeleteFailed, $"Error clearing grain state: name={this.name} grainType={grainType} grainId={grainId} ETag={grainState.ETag} Exception={ex.Message}", ex);
                throw;
            }

            if (CheckVersionInconsistency("Clear", storageVersion, ToGrainStateVersion(grainState), grainType, grainId, out var inconsistentStateException))
                throw inconsistentStateException;

            grainState.ETag = storageVersion?.ToString();
            // grainState.RecordExists = false;
            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.Trace(ErrorCode.StorageProviderBase, $"Cleared grain state: name={this.name} grainType={grainType} grainId={grainId} ETag={grainState.ETag}");
            }
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var grainId = grainReference.ToKeyString();
            var grainStateVersion = ToGrainStateVersion(grainState);
            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.Trace(ErrorCode.StorageProviderBase, $"Reading grain state: name={this.name} grainType={grainType} grainId={grainId} ETag={grainState.ETag}");
            }

            try
            {
                using var c = new SqlConnection(this.options.ConnectionString);
                var persistedGrainState = await c.QuerySingleOrDefaultAsync<PersistedGrainState>(
                    "ReadFromStorageKey",
                    param: new
                    {
                        grainId,
                        grainStateVersion
                    },
                    commandType: CommandType.StoredProcedure).ConfigureAwait(false);

                object state;
                if (persistedGrainState == null)
                {
                    logger.Info(ErrorCode.StorageProviderBase, $"Null grain state read (default will be instantiated): name={this.name} grainType={grainType} grainId={grainId} ETag={grainState.ETag}");
                    state = Activator.CreateInstance(grainState.Type);
                }
                else
                {
                    state = this.serializationManager.DeserializeFromByteArray<object>(persistedGrainState.PayloadBinary);
                }

                grainState.State = state;
                grainState.ETag = persistedGrainState?.Version?.ToString();
                // grainState.RecordExists = false;
                if (logger.IsEnabled(LogLevel.Trace))
                {
                    logger.Trace(ErrorCode.StorageProviderBase, $"Read grain state: name={this.name} grainType={grainType} grainId={grainId} ETag={grainState.ETag}");
                }
            }
            catch (Exception ex)
            {
                logger.Error(ErrorCode.StorageProvider_DeleteFailed, $"Error reading grain state: name={this.name} grainType={grainType} grainId={grainId} ETag={grainState.ETag} Exception={ex.Message}", ex);
                throw;
            }
        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var grainId = grainReference.ToKeyString();
            var grainStateVersion = ToGrainStateVersion(grainState);
            var payloadBinary = this.serializationManager.SerializeToByteArray(grainState.State);
            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.Trace(ErrorCode.StorageProviderBase, $"Writing grain state: name={this.name} grainType={grainType} grainId={grainId} ETag={grainState.ETag}");
            }

            int? storageVersion = null;
            try
            {
                using var c = new SqlConnection(this.options.ConnectionString);
                storageVersion = await c.QuerySingleOrDefaultAsync<int?>(
                    "WriteToStorageKey",
                    param: new
                    {
                        grainId,
                        grainStateVersion,
                        payloadBinary
                    },
                    commandType: CommandType.StoredProcedure).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.Error(ErrorCode.StorageProvider_DeleteFailed, $"Error writing grain state: name={this.name} grainType={grainType} grainId={grainId} ETag={grainState.ETag} Exception={ex.Message}", ex);
                throw;
            }

            if (CheckVersionInconsistency("Write", storageVersion, ToGrainStateVersion(grainState), grainType, grainId, out var inconsistentStateException))
                throw inconsistentStateException;

            grainState.ETag = storageVersion?.ToString();
            // grainState.RecordExists = false;
            if (logger.IsEnabled(LogLevel.Trace))
            {
                logger.Trace(ErrorCode.StorageProviderBase, $"Wrote grain state: name={this.name} grainType={grainType} grainId={grainId} ETag={grainState.ETag}");
            }
        }

        private int? ToGrainStateVersion(IGrainState grainState) => !string.IsNullOrWhiteSpace(grainState.ETag) ? int.Parse(grainState.ETag, CultureInfo.InvariantCulture) : default(int?);

        private bool CheckVersionInconsistency(string operation, int? storageVersion, int? grainStateVersion, string grainType, string grainId, out InconsistentStateException exception)
        {
            if (storageVersion == grainStateVersion || storageVersion == null)
            {
                exception = new InconsistentStateException($"Version conflict ({operation}): name={this.name} grainType={grainType} grainId={grainId} ETag={grainStateVersion}.");
                return true;
            }
            else
            {
                exception = null;
                return false;
            }

        }

        private class PersistedGrainState
        {
            public byte[] PayloadBinary { get; set; }

            public int? Version { get; set; }
        }
    }
}