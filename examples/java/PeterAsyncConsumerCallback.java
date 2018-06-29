import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class PeterAsyncConsumerCallback implements OffsetCommitCallback {
    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null)
            System.out.println("Commit failed: " + offsets + exception);
    }
}