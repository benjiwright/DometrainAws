using System.Net;
using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace Movies.Api;

public class DataSeeder
{
    public async Task ImportDataAsync()
    {

        var dbConfig = new AmazonDynamoDBConfig
        {
            ServiceURL = "http://localhost:8000",
        };

        var dynamoDb = new AmazonDynamoDBClient(dbConfig);


        var tables = await dynamoDb.ListTablesAsync();

        if (!tables.TableNames.Contains("movies"))
        {
            await CreateTable(dynamoDb);
        }
        else
        {
            // this is a bad idea to scan a non relational DB 💸💸💸
            var scanRequest = new ScanRequest
            {
                TableName = "movies",
            };
            var response = await dynamoDb.ScanAsync(scanRequest);
            var movies =   response.Items.Select(x => x).ToList();
            return;
        }

        var lines = await File.ReadAllLinesAsync("./movies.csv");
        for (int i = 0; i < lines.Length; i++)
        {
            if (i == 0)
            {
                continue; //Skip header
            }

            var line = lines[i];
            var commaSplit = line.Split(',');

            var title = commaSplit[0];
            var year = int.Parse(commaSplit[1]);
            var ageRestriction = int.Parse(commaSplit[2]);
            var rottenTomatoes = int.Parse(commaSplit[3]);

            var movie = new Movie
            {
                Id = Guid.NewGuid(),
                Title = title,
                AgeRestriction = ageRestriction,
                ReleaseYear = year,
                RottenTomatoesPercentage = rottenTomatoes
            };

            var movieAsJson = JsonSerializer.Serialize(movie);
            var itemAsDocument = Document.FromJson(movieAsJson);
            var itemAsAttributes = itemAsDocument.ToAttributeMap();

            var createItemRequest = new PutItemRequest
            {
                TableName = "movies",
                Item = itemAsAttributes
            };

            var response = await dynamoDb.PutItemAsync(createItemRequest);
            await Task.Delay(300);
        }
    }

    private async Task<bool> CreateTable(AmazonDynamoDBClient dynamoDb)
    {
        CreateTableRequest createRequest = new CreateTableRequest
        {
            TableName = "movies",
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
        var createResponse = await dynamoDb.CreateTableAsync(createRequest);
        var tables = await dynamoDb.ListTablesAsync();

        return createResponse.HttpStatusCode == HttpStatusCode.OK;
    }
}
