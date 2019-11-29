using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace util_dynamoupdater
{
  class Program
  {
    private static (string tableName, Dictionary<string, AttributeValue> lastEvaluatedKey, List<string> attributesToGet, Dictionary<string, Condition> scanFilter)[] deletionRules = {
      (
        "s-gp-public-adapter-stats-requests-throttled-summary-per-day", 
        null,
        new List<string>() { "Key" }, 
        new Dictionary<string, Condition>() {
          {
            "TC",
            new Condition() {
              ComparisonOperator = ComparisonOperator.LT,
              AttributeValueList = new List<AttributeValue>() {
                new AttributeValue() {
                  N = "100"
                }
              }
            }
          }
        }
      ),
      (
        "s-gp-public-adapter-stats-requests-summary-per-day",
        null,
        new List<string>() { "Key" }, 
        new Dictionary<string, Condition>() {
          {
            "PlanId",
            new Condition() {
              ComparisonOperator = ComparisonOperator.NULL
            }
          }
        }
      ),
      (
        "s-gp-public-adapter-stats-requests-per-day",
        null,
        new List<string>() { "Key" }, 
        new Dictionary<string, Condition>() {
          {
            "PlanId",
            new Condition() {
              ComparisonOperator = ComparisonOperator.NULL
            }
          }
        }
      ),
      (
        "s-gp-public-adapter-stats-requests-per-hour",
        null,
        new List<string>() { "Key" }, 
        new Dictionary<string, Condition>() {
          {
            "PlanId",
            new Condition() {
              ComparisonOperator = ComparisonOperator.NULL
            }
          }
        }
      )
    };
    
    private static Dictionary<string, Dictionary<string, AttributeValue>> LastEvaluatedKeys = new Dictionary<string, Dictionary<string, AttributeValue>>();

    static void Main(string[] args)
    {
      Console.WriteLine("DynamoDB Item Deleter");
      // initialise last keys
      if (System.IO.File.Exists("last-keys.json"))
      {
        // deserialise...
        var lastKeysJson = System.IO.File.ReadAllText("last-keys.json");
        LastEvaluatedKeys = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, Dictionary<string, AttributeValue>>>(lastKeysJson, new JsonSerializerOptions() {});
      }
      else{
        // set from defaults...
        foreach(var rule in deletionRules)
          if (rule.lastEvaluatedKey != null)
            LastEvaluatedKeys.Add(rule.tableName, rule.lastEvaluatedKey);
      }
      Console.CancelKeyPress +=  delegate {
        lock(LastEvaluatedKeys){
        // serialise to last-keys.bin
        var lastKeysJson = System.Text.Json.JsonSerializer.Serialize(LastEvaluatedKeys, new JsonSerializerOptions() {
          WriteIndented = true,
          IgnoreNullValues = true,
        });
        System.IO.File.WriteAllText("last-keys.json", lastKeysJson);
        Console.WriteLine("Bye...");
        }
      };

      var connection = new AmazonDynamoDBClient(
        region: Amazon.RegionEndpoint.APSoutheast2
      );
      var tasksRunning = new List<Task>();
      foreach(var ruleToQuery in deletionRules) {
        var rule = ruleToQuery;
        tasksRunning.Add(ScanAndDeleteTableItems(connection, rule));
      }

      Console.WriteLine("Waiting all table deletes...");
      Task.WaitAll(tasksRunning.ToArray());

      Console.WriteLine("Waiting Completed...");

    }


    
  private static async Task ScanAndDeleteTableItems(AmazonDynamoDBClient connection, (string tableName, Dictionary<string, AttributeValue> lastEvaluatedKey, List<string> attributesToGet, Dictionary<string, Condition> scanFilter) rule) {
    do {
      Dictionary<string, AttributeValue> lastEvaluatedKey = null;
      lock(LastEvaluatedKeys) {
        lastEvaluatedKey = LastEvaluatedKeys.ContainsKey(rule.tableName) ? LastEvaluatedKeys[rule.tableName] : null;
      }
      var scanResult = await connection.ScanAsync(
        new ScanRequest() {
          TableName = rule.tableName,
          AttributesToGet = rule.attributesToGet,
          ScanFilter = rule.scanFilter,
          Limit = 25,
          ExclusiveStartKey = lastEvaluatedKey
        });
      
      //Console.WriteLine($"{rule.tableName} Scanned. Returns {scanResult?.Items?.Count}.");
      if (scanResult?.LastEvaluatedKey.Count > 0)
        Console.WriteLine($"{DateTime.UtcNow.ToString("O")} - {rule.tableName} - Last Key: '{scanResult?.LastEvaluatedKey?.First().Key}':'{scanResult?.LastEvaluatedKey?.First().Value.S}'");
      // deleting records if any...
      if (scanResult?.Items?.Count > 0) {
        await connection.BatchWriteItemAsync(new Dictionary<string, List<WriteRequest>>() {
          {
            rule.tableName,
            scanResult.Items.Select(x => new WriteRequest(new DeleteRequest() {
              Key = x
            })).ToList()
          }
        });
      }
      rule.lastEvaluatedKey = scanResult.LastEvaluatedKey;
      lock(LastEvaluatedKeys){
        if (LastEvaluatedKeys.ContainsKey(rule.tableName))
          LastEvaluatedKeys.Remove(rule.tableName);
        LastEvaluatedKeys.Add(rule.tableName, rule.lastEvaluatedKey);
      }
    } while (rule.lastEvaluatedKey.Count > 0);
  }
  }

}
