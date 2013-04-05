using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace LyphTEC.Repository.MongoDB.Extensions
{
    public static class HelperExtensions
    {
        public static BsonObjectId ToBsonObjectId(this object instance)
        {
            if (ReferenceEquals(instance, null) || string.IsNullOrWhiteSpace(instance.ToString()))
                return null;

            return new BsonObjectId(new ObjectId(instance.ToString()));
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
        /// Ensures collection has indexes on specified properties (no magic strings version)
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="fieldExpression"></param>
        public static void EnsureIndex<T>(this MongoCollection<T> collection, params Expression<Func<T, object>>[] fieldExpression) where T : class
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

            collection.EnsureIndex(propNames.ToArray());
        }

        /// <summary>
        /// Ensures that the desired unique index exists on one or multiple properties, and creates it if it does not.
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="fieldExpression"></param>
        public static void EnsureUniqueIndex<T>(this MongoCollection<T> collection, params Expression<Func<T, object>>[] fieldExpression) where T : class
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

            collection.EnsureIndex(IndexKeys.Ascending(propNames.ToArray()), IndexOptions.SetUnique(true));
        }
    }
}
