﻿namespace NServiceBus.AcceptanceTests.Routing
{
    using System.Threading.Tasks;
    using AcceptanceTesting;
    using AcceptanceTesting.Customization;
    using EndpointTemplates;
    using NUnit.Framework;

    public class When_overriding_local_address : NServiceBusAcceptanceTest
    {
        public static string ReceiverEndpointName => Conventions.EndpointNamingConvention(typeof(Receiver));
        public static string ReceiverQueueName => "q_" + ReceiverEndpointName;

        [Test]
        public async Task Should_use_the_provided_address_as_input_queue_name()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<Sender>(e => e.When(c =>
                {
                    var options = new SendOptions();
                    options.SetDestination(ReceiverQueueName);
                    return c.Send(new Message(), options);
                }))
                .WithEndpoint<Receiver>()
                .Done(c => c.ReceivedMessage)
                .Run();

            Assert.That(context.ReceivedMessage, Is.True);
        }

        public class Context : ScenarioContext
        {
            public bool ReceivedMessage { get; set; }
        }

        public class Sender : EndpointConfigurationBuilder
        {
            public Sender()
            {
                EndpointSetup<DefaultServer>(c => { });
            }
        }

        public class Receiver : EndpointConfigurationBuilder
        {
            public Receiver()
            {
                EndpointSetup<DefaultServer>(c => c.OverrideLocalAddress(ReceiverQueueName));
            }

            public class MessageHandler : IHandleMessages<Message>
            {
                public MessageHandler(Context testContext)
                {
                    this.testContext = testContext;
                }

                public Task Handle(Message message, IMessageHandlerContext context)
                {
                    testContext.ReceivedMessage = true;
                    return Task.FromResult(0);
                }

                Context testContext;
            }
        }

        public class Message : ICommand
        {
        }
    }
}