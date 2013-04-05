using System;
using System.Collections.Generic;
using System.Composition;
using System.Composition.Convention;
using System.Composition.Hosting;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LyphTEC.Repository.MongoDB.Tests.Extensions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using ServiceStack.Text;
using Xunit;
using LyphTEC.Repository.Tests;
using LyphTEC.Repository.Tests.Domain;

namespace LyphTEC.Repository.MongoDB.Tests
{
    public class MongoRepositoryTests : CommonRepositoryTest,  IUseFixture<CommonRepositoryFixture>
    {
        private MongoRepository<Customer> _repo;
        private MongoDatabase _db;

        #region IUseFixture<MongoDBFixture> Members

        public void SetFixture(CommonRepositoryFixture data)
        {
            var url = MongoUrl.Create("mongodb://localhost/MongoRepository?safe=true");
            var client = new MongoClient(url);
            var server = client.GetServer();

            _db = server.GetDatabase(url.DatabaseName);

            _repo = new MongoRepository<Customer>(_db);

            CustomerRepo = _repo;
            CustomerRepoAsync = _repo;
        }

        #endregion

        public override void ClearRepo()
        {
            _repo.MongoCollection.Drop();
        }
        
        [Fact]
        public void Save_Ok()
        {
            ClearRepo();
            
            var cust = NewCustomer();
            cust.Address = NewAddress();

            CustomerRepo.Save(cust);

            Assert.Equal(CustomerRepo.Count(), 1);

            DumpRepo();
        }

        [Fact]
        public void SaveAll_Ok()
        {
            ClearRepo();

            CustomerRepo.SaveAll(NewCustomers());

            Assert.Equal(CustomerRepo.Count(), 3);
            
            DumpRepo();
        }

        [Fact]
        public void Save_Update_Ok()
        {
            ClearRepo();

            var cust = CustomerRepo.Save(NewCustomer());

            var before = cust.Email;

            cust.Email = "updated@me.com";

            var result = CustomerRepo.Save(cust);

            Assert.NotEqual(before, result.Email);

            result.PrintDump();
        }

        [Fact]
        public void One_Linq_Ok()
        {
            ClearRepo();

            CustomerRepo.SaveAll(NewCustomers());

            var actual = CustomerRepo.One(x => x.Email.Equals("jsmith@acme.com"));

            Assert.NotNull(actual);
            
            actual.PrintDump();
        }

        [Fact]
        public void RemoveById_Ok()
        {
            ClearRepo();

            CustomerRepo.SaveAll(NewCustomers());

            var one = CustomerRepo.One(x => x.Email.Equals("jsmith@acme.com"));

            Console.WriteLine("Removing Id: {0}", one.Id);
            CustomerRepo.Remove(one.Id);

            Assert.Equal(2, CustomerRepo.Count());

            var cust = CustomerRepo.One(one.Id);
            Assert.Null(cust);
            
            DumpRepo();
        }

        [Fact]
        public void Remove_Ok()
        {
            ClearRepo();

            var cust = NewCustomer();
            CustomerRepo.Save(cust);

            Assert.Equal(1, CustomerRepo.Count());
            
            CustomerRepo.Remove(cust);

            Assert.Equal(0, CustomerRepo.Count());
        }

        [Fact]
        public void RemoveByIds_Ok()
        {
            ClearRepo();

            CustomerRepo.SaveAll(NewCustomers());

            var ids = CustomerRepo.All().Take(2).Select(x => x.Id).ToList();

            Console.WriteLine("Removing Ids: ");

            ids.PrintDump();

            CustomerRepo.RemoveByIds(ids);

            Assert.Equal(1, CustomerRepo.Count());
            
            DumpRepo();
        }

        [Fact]
        public void All_Linq_Ok()
        {
            ClearRepo();

            CustomerRepo.SaveAll(NewCustomers());
            CustomerRepo.Save(NewCustomer("James", "Harrison", "jharrison@foobar.com", "FooBar"));

            Assert.Equal(4, CustomerRepo.Count());
            
            DumpRepo();

            var actual = CustomerRepo.All(x => x.Company.Equals("ACME"));

            Assert.Equal(3, actual.Count());

            Console.WriteLine("After filter: Company == 'ACME'");
            actual.PrintDump();
        }

        [Fact]
        public async Task SaveAsync_Ok()
        {
            ClearRepo();

            CustomerRepo.SaveAll(NewCustomers());

            var cust = NewCustomer("James", "Harrison", "jharrison@foobar.com", "FooBar");

            Console.WriteLine("ThreadId before await: {0}", System.Threading.Thread.CurrentThread.ManagedThreadId);

            var actual = await CustomerRepoAsync.SaveAsync(cust);

            Console.WriteLine("ThreadId after await: {0}", System.Threading.Thread.CurrentThread.ManagedThreadId);

            Assert.Equal(4, CustomerRepo.Count());

            DumpRepo();
        }

        // MEF 2 requires public members : http://mef.codeplex.com/wikipage?title=Changes&referringTitle=Documentation
        [Import]
        public IRepository<Customer> MefCustomerRepo { get; set; }
            
        [Fact]
        public void MEF_Ok()
        {
            var config = new ContainerConfiguration()
                .WithAssembly(typeof (MongoRepository<>).Assembly)
                .WithExport(_repo.MongoCollection.Database);         // Using demo feature from here : http://mef.codeplex.com/SourceControl/changeset/view/24db23e5045a#oob/demo/Microsoft.Composition.Demos.ExtendedPartTypes/Program.cs

            using (var container = config.CreateContainer())
            {
                container.SatisfyImports(this);
            }

            ClearRepo();
            CustomerRepo.SaveAll(NewCustomers());

            var cust = NewCustomer("MEF", "Head", "mef@meffy.com", "MEFFY");

            MefCustomerRepo.Save(cust);

            Assert.Equal(4, CustomerRepo.Count());
            
            DumpRepo();
        }

        [Fact]
        public void Init_WithExtraOptions_Ok()
        {
            Action extraInit = () =>
                                   {
                                       if (!BsonClassMap.IsClassMapRegistered(typeof (Employee)))
                                       {
                                           BsonClassMap.RegisterClassMap<Employee>();
                                       }

                                       // specify base Entity Id strategy to use IntIdGenerator instead of default ObjectIdGenerator
                                       var rootMap = BsonClassMap.GetRegisteredClassMaps().SingleOrDefault(x => x.ClassType == typeof (Entity));
                                       if (rootMap != null)
                                           rootMap.IdMemberMap.SetRepresentation(BsonType.Int32).SetIdGenerator(IntIdGenerator.Instance);
                                   };

            var repo = new MongoRepository<Employee>(_db, extraInitOptions: extraInit);
            
            // reset 1st
            repo.MongoCollection.Drop();
            repo.MongoDatabase.GetCollection("IDSequence").Drop();

            var emp = new Employee
                          {
                              FirstName = "Jack",
                              LastName = "Russell"
                          };

            repo.Save(emp);

            Assert.Equal(1, emp.Id);
            
            repo.All().PrintDump();
        }
    }
}
