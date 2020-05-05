package seminar;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;

public class CustomerRequestHandler implements RequestHandler<DynamodbEvent, String> {
	@Override
	public String handleRequest(DynamodbEvent dynamodbEvent, Context context) {

		final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());
		final Table table = dynamoDB.getTable("Customer");

		System.out.println("Processing #records: " + dynamodbEvent.getRecords().size());

		for (final DynamodbStreamRecord record : dynamodbEvent.getRecords()) {

			System.out.println("Event type: " + record.getEventName());

			// Ignore all events except insert.
			if (!"INSERT".equals(record.getEventName())) {
				System.out.println("Non insert event for " + record.getEventID() + ", Skipping!");
				continue;
			}

			final StreamRecord streamRecord = record.getDynamodb();
			final String customerId = streamRecord.getNewImage().get("customer_id").getS();
			final String name = streamRecord.getNewImage().get("name").getS();

			System.out.println("updating customer id: " + customerId + ", name: " + name);

			final UpdateItemSpec updateSpec = new UpdateItemSpec()
					.withPrimaryKey("customer_id", customerId, "name", name)
					.withUpdateExpression("set website = :url")
					.withValueMap(new ValueMap().withString(":url", "https://www.google.com"))
					.withReturnValues(ReturnValue.UPDATED_NEW);

			try {
				final UpdateItemOutcome outcome = table.updateItem(updateSpec);
				System.out.println("Update succeeded: " + outcome.getItem().toJSONPretty());
			} catch (final Exception e) {
				System.out.println("Update failure: " + streamRecord.getKeys());
				e.printStackTrace();
			}
		}

		dynamoDB.shutdown();

		return "success";
	}
}
