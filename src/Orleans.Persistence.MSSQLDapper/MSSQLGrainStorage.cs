// Unmodified portions of this file land under the following license
//
// The MIT License (MIT)
//
// Copyright (c) .NET Foundation
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// The design criteria for this table are:
//
// 1. It can contain arbitrary content serialized as binary
//
// 2. The table design should be scaled to support inserting large payloads quickly
//
// 3. This is accomplished with Memory Optmized OLTP tables and natively compiled stored procedures
//
// 4. The table will be entirely in Memory, so options such as JSON/XML are not supported
//
// 5. Grain state DELETE will set NULL to the data fields and updates the Version number normally.
// This should alleviate the need for index or statistics maintenance with the loss of some bytes of storage space. 
// The table can be scrubbed
// in a separate maintenance operation.
//
// 6. In the storage operations queries the columns need to be in the exact same order
// since the storage table operations support optionally streaming.
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