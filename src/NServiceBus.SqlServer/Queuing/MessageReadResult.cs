namespace NServiceBus.Transport.SQLServer
{
    struct MessageReadResult
    {
        MessageReadResult(Message message, MessageRow rawData)
        {
            Message = message;
            RawData = rawData;
        }

        public static MessageReadResult NoMessage = new MessageReadResult(null, null);

        public bool IsPoison => RawData != null && Message == null;

        public bool Successful => Message != null;

        public Message Message { get; }

        public MessageRow RawData { get; }

        public static MessageReadResult Poison(MessageRow messageRow)
        {
            return new MessageReadResult(null, messageRow);
        }

        public static MessageReadResult Success(MessageRow messageRow, Message message)
        {
            return new MessageReadResult(message, messageRow);
        }
    }
}