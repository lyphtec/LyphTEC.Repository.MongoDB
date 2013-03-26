using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;

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
    }
}
