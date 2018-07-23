using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using SendGrid.Helpers.Mail;

private const int DefaultMaximumQuota = 2000;

public static async Task<Mail> Run(TimerInfo myTimer, TraceWriter log)
{
    // list the Cosmos DB accounts available
    var accounts = await ListCosmosDBAccountsAsync(log);

    // get the alerts to fire for each Cosmos DB account
    var tasks = new List<Task<List<Alert>>>();
    foreach (var accountDetails in accounts)
    {
        tasks.Add(CheckCosmosDBAccountAsync(accountDetails.EndpointUri, accountDetails.ReadOnlyKey, log));
    }
    await Task.WhenAll(tasks);
    var allAlerts = new List<Alert>();
    foreach (var completedTask in tasks)
    {
        allAlerts.AddRange(completedTask.Result);
    }

    // send the alerts, if any
    log.Info($"Found {allAlerts.Count} alert(s) to fire.");
    if (! allAlerts.Any())
    {
        return null;
    }
    
    return CreateEmailAlert(allAlerts, log);
}

private static Mail CreateEmailAlert(List<Alert> allAlerts, TraceWriter log)
{
    var fullAlertString = "The following Cosmos DB collections may be overprovisioned:\n";
    foreach (var alert in allAlerts)
    {
        fullAlertString += $"* {alert.ToString()}\n";
    }
    
    var message = new Mail();
    message.AddContent(new Content
    {
        Type = "text/plain",
        Value = fullAlertString
    });
    return message;
}

private static async Task<IEnumerable<CosmosDBAccount>> ListCosmosDBAccountsAsync(TraceWriter log)
{
    var accountList = new List<CosmosDBAccount>();

    // get the function's credentials from its managed service identity
    var credentials = new AzureCredentialsFactory()
        .FromMSI(new MSILoginInformation(MSIResourceType.AppService), AzureEnvironment.AzureGlobalCloud);

    // get a list of all subscription IDs accessible to the logged in principal
    var azure = Azure.Configure()
        .Authenticate(credentials);
    var subscriptions = await azure
        .Subscriptions
        .ListAsync();
    var subscriptionIds = subscriptions.Select(s => s.SubscriptionId);
            
    // find all Cosmos DB accounts within each subscription
    var tasks = new List<Task<IEnumerable<CosmosDBAccount>>>();
    foreach (var subscriptionId in subscriptionIds)
    {
        tasks.Add(ListCosmosDBAccountsInSubscriptionAsync(subscriptionId, credentials, log));
    }
    await Task.WhenAll(tasks);
    foreach (var task in tasks)
    {
        accountList.AddRange(task.Result);
    }

    return accountList;
}

private static async Task<IEnumerable<CosmosDBAccount>> ListCosmosDBAccountsInSubscriptionAsync(string subscriptionId, AzureCredentials credentials, TraceWriter log)
{
    var accountList = new List<CosmosDBAccount>();

    // connect to the Azure subscription
    var azure = Azure
            .Configure()
            .Authenticate(credentials)
            .WithSubscription(subscriptionId);
    log.Verbose($"Checking subscription '{subscriptionId}'");
            
    // list all Cosmos DB accounts within the subscription
    var accounts = await azure.CosmosDBAccounts.ListAsync();
    foreach (var account in accounts)
    {
        // get the account endpoint URI and read-only key
        var endpointUri = account.DocumentEndpoint;
        var authKeys = await account.ListReadOnlyKeysAsync();

        accountList.Add(new CosmosDBAccount
        {
            EndpointUri = endpointUri,
            ReadOnlyKey = authKeys.PrimaryReadonlyMasterKey
        });
    }

    return accountList;
}

private static async Task<List<Alert>> CheckCosmosDBAccountAsync(string endpointUri, string authKeyString, TraceWriter log)
{
    var alerts = new List<Alert>();

    // connect to the Cosmos DB account
    var client = new DocumentClient(new Uri(endpointUri), authKeyString);
    var account = await client.GetDatabaseAccountAsync();
    log.Verbose($"Scanning Cosmos DB account '{account.Id}'");

    // get a list of databases in the account
    var databases = await client.ReadDatabaseFeedAsync();

    // get a list of offers, each of which represent the throughput of a collection
    var offers = await client.ReadOffersFeedAsync();

    foreach (var database in databases)
    {
        // get a list of collections within the database
        var collections = await client.ReadDocumentCollectionFeedAsync(database.CollectionsLink);

        foreach (var collection in collections)
        {
            log.Verbose($"Checking collection '{collection.Id}' in database '{database.Id}'");
            long quota;

            // find the quota (throughput) for the collection
            var collectionOffer = offers.SingleOrDefault(o => o.ResourceLink == collection.SelfLink);
            if (collectionOffer is OfferV2)
            {
                quota = ((OfferV2)collectionOffer).Content.OfferThroughput;
            }
            else
            {
                var offer = await client.ReadOfferAsync(collectionOffer.SelfLink);
                quota = offer.CollectionQuota;
            }
            
            // check the throughput against the policy for the collection
            var alert = CreateAlert(quota, account.Id, database.Id, collection.Id, log);
            if (alert != null)
            {
                alerts.Add(alert);
            }
        }
    }

    return alerts;
}

private static Alert CreateAlert(long quota, string accountId, string databaseId, string collectionId, TraceWriter log)
{
    var maximumQuota = GetMaximumQuotaForCollection(accountId, databaseId, collectionId);

    if (quota > maximumQuota)
    {
        log.Info($"Firing alert for collection '{collectionId}' in database '{databaseId}' in account '{accountId}'. Expected maximum throughput to be {maximumQuota}, actual throughput {quota}.");

        return new Alert
        { 
            ActualQuota = quota,
            MaximumQuota = maximumQuota,
            AccountId = accountId,
            DatabaseId = databaseId,
            CollectionId = collectionId
        };
    }

    return null;
}

private static long GetMaximumQuotaForCollection(string accountId, string databaseId, string collectionId)
{
    var settingName = $"MaximumThroughput:{accountId}:{databaseId}:{collectionId}";
    var quotaSetting = System.Environment.GetEnvironmentVariable(settingName, EnvironmentVariableTarget.Process);

    if (quotaSetting != null && long.TryParse(quotaSetting, out var quota))
    {
        return quota;
    }
    else
    {
        return DefaultMaximumQuota;
    }    
}

class CosmosDBAccount
{
    public string EndpointUri { get; set; }
    public string ReadOnlyKey { get; set; }
}

class Alert
{
    public long ActualQuota { get; set; }
    public long MaximumQuota { get; set; }
    public string AccountId { get; set; }
    public string DatabaseId { get; set; }
    public string CollectionId { get; set; }

    public override string ToString() => $"`{CollectionId}` (in `{AccountId}/{DatabaseId}`) - expected maximum {MaximumQuota} RU/s, currently {ActualQuota} RU/s";
}
