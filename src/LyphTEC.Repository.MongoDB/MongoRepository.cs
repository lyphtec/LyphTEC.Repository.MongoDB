using System;
using System.Collections.Generic;
using System.Composition;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using LyphTEC.Repository.Extensions;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Driver.Linq;

namespace LyphTEC.Repository.MongoDB
{
    /// <summary>
    /// <see cref="IRepository{TEntity}"/> implementation using MongoDB as the backing store
    /// </summary>
    /// <typeparam name="TEntity"></typeparam>
    [Export(typeof(IRepository<>))]
    [Export(typeof(IRepositoryAsync<>))]
    [Shared]
    public class MongoRepository<TEntity> : IRepository<TEntity>, IRepositoryAsync<TEntity>
        where TEntity : class, IEntity
    {
        private readonly MongoDatabase _db;
        private readonly MongoCollection<TEntity> _col;

        /// <summary>
        /// Instantiates a new instance of <see cref="MongoRepository{TEntity}"/>
        /// </summary>
        /// <param name="db">MongoDatabase to use</param>
        /// <param name="initDefaultOptions">Whether to call <see cref="InitMongo"/> on instantiation. Default is true. This allows you to override the default mappings, serialization options, and entity Id representation.</param>
        [ImportingConstructor]
        public MongoRepository([Import]MongoDatabase db, [Import("InitMongoDefaultOptions", AllowDefault = true)]bool initDefaultOptions = true)
        {
            Contract.Requires<ArgumentNullException>(db != null);

            _db = db;
            _col = _db.GetCollection<TEntity>(typeof (TEntity).Name);

            if (initDefaultOptions)
                InitMongo();
        }

        /// <summary>
        /// Gets the MongoDatabase
        /// </summary>
        internal MongoDatabase MongoDatabase { get { return _db; } }

        /// <summary>
        /// Gets the MongoCollection{TEntity}
        /// </summary>
        public MongoCollection<TEntity> MongoCollection { get { return _col; } }

        /// <summary>
        /// Ensures collection has indexes on specified properties (no magic strings version)
        /// </summary>
        /// <param name="fieldExpression"></param>
        public void EnsureIndex(params Expression<Func<TEntity, object>>[] fieldExpression)
        {
            var propNames = new List<string>();

            var counter = 1;
            foreach (var pi in fieldExpression.Select(GetPropertyInfo))
            {
                if (pi == null)
                    throw new InvalidOperationException(string.Format("Expression {0} must be a property reference", counter));

                propNames.Add(pi.Name);

                counter++;
            }

            _col.EnsureIndex(propNames.ToArray());
        }

        // ideas from http://stackoverflow.com/questions/671968/retrieving-property-name-from-lambda-expression
        static PropertyInfo GetPropertyInfo(Expression exp)
        {
            var lamb = exp as LambdaExpression;
            if (lamb == null)
                throw new InvalidOperationException("exp must be a LambdaExpression");

            var body = lamb.Body as MemberExpression;

            if (body == null)
            {
                var ubody = (UnaryExpression)lamb.Body;
                body = ubody.Operand as MemberExpression;
            }

            return body == null ? null : body.Member as PropertyInfo;
        }

        /// <summary>
        /// Ensures that the desired unique index exists and creates it if it does not.
        /// </summary>
        /// <param name="fieldExpression"></param>
        public void EnsureUniqueIndex(Expression<Func<TEntity, object>> fieldExpression)
        {
            var pi = GetPropertyInfo(fieldExpression);

            if (pi == null)
                throw new InvalidOperationException("Expression must be a property reference");

            _col.EnsureIndex(IndexKeys.Ascending(pi.Name), IndexOptions.SetUnique(true));
        }

        #region IRepository<TEntity> Members

        public virtual IQueryable<TEntity> All(Expression<Func<TEntity, bool>> predicate = null)
        {
            return predicate == null ? _col.AsQueryable() : _col.AsQueryable().Where(predicate);
        }

        public virtual bool Any(Expression<Func<TEntity, bool>> predicate = null)
        {
            return predicate == null ? _col.AsQueryable().Any() : _col.AsQueryable().Any(predicate);
        }

        public virtual int Count(Expression<Func<TEntity, bool>> predicate = null)
        {
            return predicate == null ? _col.AsQueryable().Count() : _col.AsQueryable().Count(predicate);
        }

        public virtual TEntity One(Expression<Func<TEntity, bool>> predicate)
        {
            return _col.AsQueryable().SingleOrDefault(predicate);
        }

        public virtual TEntity One(object id)
        {
            return _col.FindOneByIdAs<TEntity>(new BsonObjectId(id.ToString()));
        }

        public virtual void Remove(object id)
        {
            _col.Remove(Query.EQ("_id", new BsonObjectId(id.ToString())));
        }

        public virtual void Remove(TEntity entity)
        {
            Remove(entity.Id);
        }

        public virtual void RemoveAll()
        {
            _col.RemoveAll();
        }

        public virtual void RemoveByIds(System.Collections.IEnumerable ids)
        {
            var query = Query.In("_id", ids.Cast<object>().Select(x => new BsonObjectId(x.ToString())));
            _col.Remove(query);
        }

        public virtual TEntity Save(TEntity entity)
        {
            if (entity == null)
                return null;

            _col.Save(entity);

            return entity;
        }

        public virtual void SaveAll(IEnumerable<TEntity> entities)
        {
            if (entities == null || !entities.Any())
                return;

            // InsertBatch can be more efficient
            var toInsert = entities.Where(x => x.Id == null);
            if (toInsert.Any())
                _col.InsertBatch(toInsert);

            // Loop thru & save the rest
            entities.Where(x => x.Id != null).ForEach(x => Save(x));
        }

        #endregion
        
        #region IRepositoryAsync<TEntity> Members

        // TODO: Async API are currently only TaskCompletionSource wrappers around the sync API. When official MongoDB CSharp driver supports true async, we will use that instead
        // See https://jira.mongodb.org/browse/CSHARP-138 for details

        public virtual Task<IQueryable<TEntity>> AllAsync(Expression<Func<TEntity, bool>> predicate = null)
        {
            var tcs = new TaskCompletionSource<IQueryable<TEntity>>();

            try
            {
                tcs.SetResult(All(predicate));
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            return tcs.Task;
        }

        public virtual Task<bool> AnyAsync(Expression<Func<TEntity, bool>> predicate = null)
        {
            var tcs = new TaskCompletionSource<bool>();

            try
            {
                tcs.SetResult(Any(predicate));
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            return tcs.Task;
        }

        public virtual Task<int> CountAsync(Expression<Func<TEntity, bool>> predicate = null)
        {
            var tcs = new TaskCompletionSource<int>();

            try
            {
                tcs.SetResult(Count(predicate));
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            return tcs.Task;
        }

        public virtual Task<TEntity> OneAsync(Expression<Func<TEntity, bool>> predicate)
        {
            var tcs = new TaskCompletionSource<TEntity>();

            try
            {
                tcs.SetResult(One(predicate));
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            return tcs.Task;
        }

        public virtual Task<TEntity> OneAsync(object id)
        {
            var tcs = new TaskCompletionSource<TEntity>();

            try
            {
                tcs.SetResult(One(id));
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            return tcs.Task;
        }

        public virtual Task<bool> RemoveAllAsync()
        {
            var tcs = new TaskCompletionSource<bool>();

            try
            {
                RemoveAll();
                tcs.SetResult(true);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            return tcs.Task;
        }

        public virtual Task<bool> RemoveAsync(object id)
        {
            var tcs = new TaskCompletionSource<bool>();

            try
            {
                Remove(id);
                tcs.SetResult(true);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            return tcs.Task;
        }

        public virtual Task<bool> RemoveAsync(TEntity entity)
        {
            var tcs = new TaskCompletionSource<bool>();

            try
            {
                Remove(entity);
                tcs.SetResult(true);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            return tcs.Task;
        }

        public virtual Task<bool> RemoveByIdsAsync(System.Collections.IEnumerable ids)
        {
            var tcs = new TaskCompletionSource<bool>();

            try
            {
                RemoveByIds(ids);
                tcs.SetResult(true);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            return tcs.Task;
        }

        public virtual Task<bool> SaveAllAsync(IEnumerable<TEntity> entities)
        {
            var tcs = new TaskCompletionSource<bool>();

            try
            {
                SaveAll(entities);
                tcs.SetResult(true);
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            return tcs.Task;
        }

        public virtual Task<TEntity> SaveAsync(TEntity entity)
        {
            var tcs = new TaskCompletionSource<TEntity>();

            try
            {
                Debug.WriteLine("SaveAsync() ThreadId: {0}", System.Threading.Thread.CurrentThread.ManagedThreadId);

                tcs.SetResult(Save(entity));
            }
            catch (Exception ex)
            {
                tcs.TrySetException(ex);
            }

            return tcs.Task;
        }

        #endregion

        private static bool _initialized = false;

        /// <summary>
        /// Configures default Mongo mappings and serialization options
        /// </summary>
        public static void InitMongo()
        {
            if (_initialized)
                return;

            if (!BsonClassMap.IsClassMapRegistered(typeof (Entity)))
            {
                BsonClassMap.RegisterClassMap<Entity>(cm =>
                                                          {
                                                              cm.AutoMap();
                                                              cm.SetIdMember(cm.GetMemberMap(x => x.Id));
                                                              cm.IdMemberMap.SetRepresentation(BsonType.ObjectId).SetIdGenerator(ObjectIdGenerator.Instance);
                                                              cm.SetIgnoreExtraElements(true);
                                                              cm.SetIgnoreExtraElementsIsInherited(true);
                                                              cm.SetIsRootClass(true);
                                                              cm.GetMemberMap(x => x.DateCreatedUtc).SetSerializationOptions(new DateTimeSerializationOptions(DateTimeKind.Utc, BsonType.Document));
                                                              cm.GetMemberMap(x => x.DateUpdatedUtc).SetSerializationOptions(new DateTimeSerializationOptions(DateTimeKind.Utc, BsonType.Document));
                                                          });
            }

            BsonSerializer.UseNullIdChecker = true;
            BsonSerializer.UseZeroIdChecker = true;

            // DateTime serialization handling & precision in MongoDB : http://alexmg.com/post/2011/09/30/DateTime-precision-with-MongoDB-and-the-C-Driver.aspx 
            // Also from official docs : http://www.mongodb.org/display/DOCS/CSharp+Driver+Serialization+Tutorial#CSharpDriverSerializationTutorial-DateTimeSerializationOptions
            DateTimeSerializationOptions.Defaults = new DateTimeSerializationOptions(DateTimeKind.Utc, BsonType.Document);

            _initialized = true;
        }
    }
}
