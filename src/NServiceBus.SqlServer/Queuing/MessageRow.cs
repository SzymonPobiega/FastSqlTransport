namespace NServiceBus.Transport.SQLServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.IO;
    using System.Threading.Tasks;
    using Logging;
    using static System.String;

    class MessageRow
    {
        public int Seq { get; private set; }

        MessageRow() { }

        public static async Task<MessageReadResult> Read(SqlDataReader dataReader)
        {
            var row = await ReadRow(dataReader).ConfigureAwait(false);
            return row.TryParse();
        }

        public static MessageRow From(string messageId, Dictionary<string, string> headers, byte[] body, int currentSeq)
        {
            return new MessageRow
            {
                Seq = currentSeq,
                id = messageId,
                headers = DictionarySerializer.Serialize(headers),
                bodyBytes = body
            };
        }

        public void UpdateSeq(int newSeq)
        {
            Seq = newSeq;
        }

        public void PrepareSendCommand(SqlCommand command)
        {
            AddParameter(command, "seq", SqlDbType.Int, Seq);
            AddParameter(command, "id", SqlDbType.VarChar, id);
            AddParameter(command, "headers", SqlDbType.VarChar, headers);
            AddParameter(command, "body", SqlDbType.VarBinary, bodyBytes);
        }

        static async Task<MessageRow> ReadRow(SqlDataReader dataReader)
        {
            //HINT: we are assuming that dataReader is sequential. Order or reads is important !
            return new MessageRow
            {
                Seq = await dataReader.GetFieldValueAsync<int>(0).ConfigureAwait(false),
                id = await dataReader.GetFieldValueAsync<string>(1).ConfigureAwait(false),
                headers = await GetHeaders(dataReader, 2).ConfigureAwait(false),
                bodyBytes = await GetBody(dataReader, 3).ConfigureAwait(false)
            };
        }

        MessageReadResult TryParse()
        {
            try
            {
                var parsedHeaders = IsNullOrEmpty(headers)
                    ? new Dictionary<string, string>()
                    : DictionarySerializer.DeSerialize(headers);

                return MessageReadResult.Success(this, new Message(id, parsedHeaders, bodyBytes));
            }
            catch (Exception ex)
            {
                Logger.Error("Error receiving message. Probable message metadata corruption. Moving to error queue.", ex);
                return MessageReadResult.Poison(this);
            }
        }
        
        static async Task<string> GetHeaders(SqlDataReader dataReader, int headersIndex)
        {
            if (await dataReader.IsDBNullAsync(headersIndex).ConfigureAwait(false))
            {
                return null;
            }

            using (var textReader = dataReader.GetTextReader(headersIndex))
            {
                return await textReader.ReadToEndAsync().ConfigureAwait(false);
            }
        }

        static async Task<byte[]> GetBody(SqlDataReader dataReader, int bodyIndex)
        {
            // Null values will be returned as an empty (zero bytes) Stream.
            using (var outStream = new MemoryStream())
            using (var stream = dataReader.GetStream(bodyIndex))
            {
                await stream.CopyToAsync(outStream).ConfigureAwait(false);
                return outStream.ToArray();
            }
        }

        static void AddParameter(SqlCommand command, string name, SqlDbType type, object value)
        {
            command.Parameters.Add(name, type).Value = value ?? DBNull.Value;
        }

        string id;
        string headers;
        byte[] bodyBytes;

        static ILog Logger = LogManager.GetLogger(typeof(MessageRow));
    }
}