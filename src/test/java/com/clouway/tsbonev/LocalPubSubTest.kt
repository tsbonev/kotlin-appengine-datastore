package com.clouway.tsbonev

import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.Publisher
import com.google.cloud.pubsub.v1.Subscriber
import com.google.protobuf.ByteString
import com.google.pubsub.v1.ProjectTopicName
import com.google.pubsub.v1.PubsubMessage
import org.junit.Test
import com.google.cloud.pubsub.v1.TopicAdminSettings
import com.google.cloud.pubsub.v1.TopicAdminClient
import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import io.grpc.ManagedChannelBuilder
import org.hamcrest.CoreMatchers.`is` as Is
import java.util.concurrent.LinkedBlockingDeque


class LocalPubSubTest {

    // gcloud beta emulators pubsub start
    // gcloud beta emulators pubsub env-init
    //  Sample output:
    //  export PUBSUB_EMULATOR_HOST=localhost:8759

    val messages = LinkedBlockingDeque<PubsubMessage>()

    fun printMessage(message: PubsubMessage, consumer: AckReplyConsumer) {
        messages.offer(message)
        consumer.ack()
    }

    @Test
    fun publishToTopic() {

        val printer = {message: PubsubMessage, consumer: AckReplyConsumer ->
            printMessage(message, consumer)
        }
        val hostport = "localhost:8085"
        val channel = ManagedChannelBuilder.forTarget(hostport).build()

        try {
            val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
            val credentialsProvider = NoCredentialsProvider.create()

            // Set the channel and credentials provider when creating a `TopicAdminClient`.
            // Similarly for SubscriptionAdminClient
            val topicClient = TopicAdminClient.create(
                    TopicAdminSettings.newBuilder()
                            .setTransportChannelProvider(channelProvider)
                            .setCredentialsProvider(credentialsProvider)
                            .build())

            val topicName = ProjectTopicName.of("my-project-id", "my-topic-id")
            // Set the channel and credentials provider when creating a `Publisher`.
            // Similarly for Subscriber
            val publisher = Publisher.newBuilder(topicName)
                    .setChannelProvider(channelProvider)
                    .setCredentialsProvider(credentialsProvider)
                    .build()

            val subscriber = Subscriber.newBuilder("my-topic-id", MessageReceiver(printer))
                    .build().startAsync().awaitRunning()

            val message = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("Hello")).build()

            publisher.publish(message)

        } finally {
            channel.shutdown()
        }
    }

}