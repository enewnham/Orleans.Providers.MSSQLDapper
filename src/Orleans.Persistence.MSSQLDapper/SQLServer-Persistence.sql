-- The design criteria for this table are:
--
-- 1. It can contain arbitrary content serialized as binary
--
-- 2. The table design should be scaled to support inserting large payloads quickly
--
-- 3. This is accomplished with Memory Optmized OLTP tables and natively compiled stored procedures
--
-- 4. The table will be entirely in Memory, so options such as JSON/XML are not supported
--
-- 5. Grain state DELETE will set NULL to the data fields and updates the Version number normally.
-- This should alleviate the need for index or statistics maintenance with the loss of some bytes of storage space. 
-- The table can be scrubbed
-- in a separate maintenance operation.
--
-- 6. In the storage operations queries the columns need to be in the exact same order
-- since the storage table operations support optionally streaming.

-- Since this is a Memory optimized table you will need to add a supporting file group. E.G.
-- ALTER DATABASE ORLEANS ADD FILEGROUP ORLEANS_mod CONTAINS MEMORY_OPTIMIZED_DATA
-- ALTER DATABASE ORLEANS ADD FILE (name='ORLEANS_mod', filename='D:\SQL_DATA\ORLEANS_mod') TO FILEGROUP ORLEANS_mod 

CREATE TABLE OrleansStorageOLTP
(
    GrainId VARCHAR( 128 ) NOT NULL,

    -- One is free to alter the size of these fields.
    PayloadBinary VARBINARY(MAX) NULL,

    -- The version of the stored payload.
    Version INT NULL,

    CONSTRAINT PK_OrleansStorageOLTP
    PRIMARY KEY NONCLUSTERED (GrainId),
)
WITH
(
    MEMORY_OPTIMIZED=ON
);

GO

-- When Orleans is running in normal, non-split state, there will
-- be only one grain with the given ID and type combination only. This
-- grain saves states mostly serially if Orleans guarantees are upheld. Even
-- if not, the updates should work correctly due to version number.
--
-- In split brain situations there can be a situation where there are two or more
-- grains with the given ID and type combination. When they try to INSERT
-- concurrently, the table needs to be locked pessimistically before one of
-- the grains gets @GrainStateVersion = 1 in return and the other grains will fail
-- to update storage. The following arrangement is made to reduce locking in normal operation.
--
-- If the version number explicitly returned is still the same, Orleans interprets it so the update did not succeed
-- and throws an InconsistentStateException.
--
-- See further information at https://dotnet.github.io/orleans/Documentation/Core-Features/Grain-Persistence.html.
CREATE PROCEDURE dbo.WriteToStorageKey
(
    @GrainId VARCHAR( 128 ) NOT NULL,
    @GrainStateVersion INT NULL,
    @PayloadBinary VARBINARY(MAX) NULL
)
WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER  
AS BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = 'english')  
    -- If the @GrainStateVersion is not zero, this branch assumes it exists in this database.
    -- The NULL value is supplied by Orleans when the state is new.
    IF @GrainStateVersion IS NOT NULL
    BEGIN
        UPDATE dbo.OrleansStorageOLTP
        SET
            PayloadBinary = @PayloadBinary,
            Version = Version + 1,
            @GrainStateVersion = Version + 1
        OUTPUT Inserted.Version
        WHERE
            GrainId = @GrainId
            AND Version = @GrainStateVersion;
    END

    -- The grain state has not been read. The following locks rather pessimistically
    -- to ensure only one INSERT succeeds.
    IF @GrainStateVersion IS NULL
    BEGIN
        INSERT INTO dbo.OrleansStorageOLTP
        (
            GrainId,
            PayloadBinary,
            Version
        )
        OUTPUT Inserted.Version
        SELECT
            @GrainId,
            @PayloadBinary,
            1
        WHERE NOT EXISTS
        (
            -- There should not be any version of this grain state.
            SELECT 1
            FROM dbo.OrleansStorageOLTP
            WHERE GrainId = @GrainId
        );
    END
END;
GO

CREATE PROCEDURE dbo.ClearStorageKey
(
    @GrainId VARCHAR( 128 ) NOT NULL,
    @GrainStateVersion INT NOT NULL
)
WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER  
AS BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = 'english')  
    UPDATE dbo.OrleansStorageOLTP
    SET
        PayloadBinary = NULL,
        Version = Version + 1
    OUTPUT Inserted.Version
    WHERE
        GrainId = @GrainId
        AND Version IS NOT NULL AND Version = @GrainStateVersion AND @GrainStateVersion IS NOT NULL;
END;
GO

CREATE PROCEDURE dbo.ReadFromStorageKey
(
    @GrainId VARCHAR( 128 ) NOT NULL
)
WITH NATIVE_COMPILATION, SCHEMABINDING, EXECUTE AS OWNER  
AS BEGIN ATOMIC WITH (TRANSACTION ISOLATION LEVEL = SNAPSHOT, LANGUAGE = 'english')  
    SELECT
        PayloadBinary,
        Version
    FROM dbo.OrleansStorageOLTP
    WHERE
        GrainId = @GrainId
END;
GO
