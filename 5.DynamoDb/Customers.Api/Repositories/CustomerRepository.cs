using System.Net;
using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Customers.Api.Contracts.Data;

namespace Customers.Api.Repositories;

public class CustomerRepository : ICustomerRepository
{
    private readonly IAmazonDynamoDB _dynamoDb;
    private readonly string _tableName = "customers";

    public CustomerRepository(IAmazonDynamoDB dynamoDb)
    {
        _dynamoDb = dynamoDb;
    }

    public async Task<bool> CreateAsync(CustomerDto customer)
    {
        customer.LastUpdated = DateTime.UtcNow;
        var customerAsJson = JsonSerializer.Serialize(customer);
        var customerAsAttributes = Document.FromJson(customerAsJson).ToAttributeMap();

        var createItemRequest = new PutItemRequest
        {
            TableName = _tableName,
            Item = customerAsAttributes,
            ConditionExpression = "attribute_not_exists(pk) AND attribute_not_exists(sk)",
        };

        var response = await _dynamoDb.PutItemAsync(createItemRequest);
        return response.HttpStatusCode == HttpStatusCode.OK;
    }

    public async Task<CustomerDto?> GetAsync(Guid id)
    {
        var getItemRequest = new GetItemRequest
        {
            TableName = _tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                { "pk", new AttributeValue { S = id.ToString() } },
                { "sk", new AttributeValue { S = id.ToString() } },
            },
        };

        var response = await _dynamoDb.GetItemAsync(getItemRequest, new CancellationTokenSource(5_000).Token);

        if(response.Item.Count == 0)
        {
            return null;
        }

        var itemAsDocument = Document.FromAttributeMap(response.Item);
        return JsonSerializer.Deserialize<CustomerDto>(itemAsDocument.ToJson());
    }

    public async Task<IEnumerable<CustomerDto>> GetAllAsync()
    {
        // this is a bad idea to scan a non relational DB 💸💸💸
        var scanRequest = new ScanRequest
        {
            TableName = _tableName,
        };
        var response = await _dynamoDb.ScanAsync(scanRequest);
        return response.Items.Select(x =>
        {
            var json = Document.FromAttributeMap(x).ToJson();
            return JsonSerializer.Deserialize<CustomerDto>(json);
        })!;
    }

    public async Task<bool> UpdateAsync(CustomerDto customer,DateTime requestTime)
    {
        customer.LastUpdated = DateTime.UtcNow;
        var customerAsJson = JsonSerializer.Serialize(customer);
        var customerAsAttributes = Document.FromJson(customerAsJson).ToAttributeMap();

        var updateItemRequest = new PutItemRequest
        {
            TableName = _tableName,
            Item = customerAsAttributes,
            // prevent overwriting data with race conditions
            ConditionExpression = "lastUpdated < requestTime",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                { ":requestTime", new AttributeValue { S = requestTime.ToString("O") } },
            },

        };

        var response = await _dynamoDb.PutItemAsync(updateItemRequest);
        return response.HttpStatusCode == HttpStatusCode.OK;
    }

    public async Task<bool> DeleteAsync(Guid id)
    {
        var deleteItemRequest = new DeleteItemRequest
        {
            TableName = _tableName,
            Key = new Dictionary<string, AttributeValue>
            {
                { "pk", new AttributeValue { S = id.ToString() } },
                { "sk", new AttributeValue { S = id.ToString() } },
            },
        };

        var response = await _dynamoDb.DeleteItemAsync(deleteItemRequest);
        return response.HttpStatusCode == HttpStatusCode.OK;
    }

    private async Task<bool> MyShittyWayToHackRiderAndDocker()
    {
        // Define your table schema
        CreateTableRequest createRequest = new CreateTableRequest
        {
            TableName = "customers",
            AttributeDefinitions = new List<AttributeDefinition>
            {
                new AttributeDefinition
                {
                    AttributeName = "pk",
                    AttributeType = ScalarAttributeType.S,
                },
                new AttributeDefinition
                {
                    AttributeName = "sk",
                    AttributeType = ScalarAttributeType.S,
                },
            },
            KeySchema = new List<KeySchemaElement>
            {
                new KeySchemaElement
                {
                    AttributeName = "pk",
                    KeyType = KeyType.HASH,
                },
                new KeySchemaElement
                {
                    AttributeName = "sk",
                    KeyType = KeyType.RANGE,
                },
            },
            ProvisionedThroughput = new ProvisionedThroughput
            {
                ReadCapacityUnits = 5,
                WriteCapacityUnits = 5,
            }
        };

        // Create the table
        var createResponse = await _dynamoDb.CreateTableAsync(createRequest);
        var tables = await _dynamoDb.ListTablesAsync();

        return createResponse.HttpStatusCode == HttpStatusCode.OK;
    }
}
