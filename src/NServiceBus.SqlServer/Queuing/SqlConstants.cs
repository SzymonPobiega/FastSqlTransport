#pragma warning disable 1591
namespace NServiceBus.Transport.SQLServer
{
    using System;

    /// <summary>
    /// Not for public use.
    /// </summary>
    [Obsolete("Not for public use.")]
    public static class SqlConstants
    {
        public static readonly string PurgeText = @"
UPDATE {0} SET 
    HasMessage = 0,
    MessageId = NULL,
    Headers = NULL,
    Body = NULL;";

        public static readonly string SendText =
            @"
DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON'
SET NOCOUNT ON;

WITH message AS (
    SELECT TOP(1) *
    FROM {0} WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE HasMessage = 0
		AND Seq > @seq
    ORDER BY Seq)
UPDATE message SET HasMessage = 1, Body = @body, Headers = @headers, MessageId = @id
OUTPUT
    deleted.Seq;

IF (@NOCOUNT = 'ON') SET NOCOUNT ON;
IF (@NOCOUNT = 'OFF') SET NOCOUNT OFF;";

        public static readonly string ReceiveText = @"
DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON';
SET NOCOUNT ON;

WITH message AS (
    SELECT TOP(1) *
    FROM {0} WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE HasMessage = 1
		AND Seq > @seq
    ORDER BY Seq)
UPDATE message SET HasMessage = 0, Body = NULL, Headers = NULL, MessageId = NULL
OUTPUT
    deleted.Seq,
	deleted.MessageId,
    deleted.Headers,
    deleted.Body;

IF (@NOCOUNT = 'ON') SET NOCOUNT ON;
IF (@NOCOUNT = 'OFF') SET NOCOUNT OFF;";

        public static readonly string CreateQueueText = @"
IF EXISTS (
    SELECT * 
    FROM {1}.sys.objects 
    WHERE object_id = OBJECT_ID(N'{0}') 
        AND type in (N'U'))
RETURN

EXEC sp_getapplock @Resource = '{0}_lock', @LockMode = 'Exclusive'

IF EXISTS (
    SELECT *
    FROM {1}.sys.objects
    WHERE object_id = OBJECT_ID(N'{0}')
        AND type in (N'U'))
BEGIN
    EXEC sp_releaseapplock @Resource = '{0}_lock'
    RETURN
END

CREATE TABLE {0} (
    Seq int NOT NULL,
    HasMessage bit NOT NULL,
    MessageId varchar(200) NULL,
    Headers varchar(max) NULL,
    Body varbinary(max) NULL,
) ON [PRIMARY];

CREATE CLUSTERED INDEX Index_Seq ON {0}
(
    Seq ASC
) ON [PRIMARY];

DECLARE @count int = 0;
DECLARE @target int = 20000;
DECLARE @index int = @count;

WHILE @index < @target
BEGIN
	INSERT INTO {0} (Seq, HasMessage) VALUES (@index, 0)
	SET @index = @index + 1;
END;
";
    }
}