using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LyphTEC.Repository.Tests.Domain;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace LyphTEC.Repository.MongoDB.Tests
{
    public class MongoRepositoryFixture
    {
        private static MongoDatabase GetDatabase()
        {
            var url = MongoUrl.Create("mongodb://localhost/MongoRepository?safe=true");
            var client = new MongoClient(url);
            var server = client.GetServer();

            return server.GetDatabase(url.DatabaseName);
        }

        public MongoRepository<Customer> GetCustomerRepo()
        {
            var repo = new MongoRepository<Customer>(GetDatabase());
            
            return repo;
        }
    }
}
