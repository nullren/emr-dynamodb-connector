package nullren.global.table.hack.hive.dynamodb;

import nullren.global.table.hack.hive.dynamodb.type.HiveDynamoDBListTypeFactory;
import nullren.global.table.hack.hive.dynamodb.type.HiveDynamoDBType;

public class DynamoDBListSerDe extends DynamoDBSerDe {
  @Override
  protected HiveDynamoDBType getTypeObjectFromHiveType(String type) {
    return HiveDynamoDBListTypeFactory.getTypeObjectFromHiveType(type);
  }
}
