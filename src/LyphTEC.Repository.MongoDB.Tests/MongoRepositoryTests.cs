using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;
using ServiceStack.Text;
using Xunit;
using LyphTEC.Repository.Tests.Domain;

namespace LyphTEC.Repository.MongoDB.Tests
{
    public class MongoRepositoryTests : IUseFixture<MongoRepositoryFixture>
    {
        private MongoRepository<Customer> _repo;

        #region IUseFixture<MongoDBFixture> Members

        public void SetFixture(MongoRepositoryFixture data)
        {
            _repo = data.GetCustomerRepo();
        }

        #endregion

        private static Customer NewCustomer(string firstName = "John", string lastName = "Smith", string email = "jsmith@acme.com", string company = "ACME")
        {
            var cust = new Customer
            {
                FirstName = firstName,
                LastName = lastName,
                Company = company,
                Email = email
            };

            return cust;
        }

        [Fact]
        public void Save_Succeeds()
        {
            _repo.MongoCollection.Drop();
            
            var cust = NewCustomer();

            _repo.Save(cust);

            Assert.True(_repo.Count() == 1);

            cust.PrintDump();
        }
    }
}
