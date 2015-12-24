var Parse = require("parse/node").Parse;
var _ = require("underscore");

function hashObject(object){
  return object.className+object.id;
}

function activateParseApp(configuration)
{
  Parse.initialize(configuration.applicationId, configuration.javascriptKey, configuration.masterKey);
  Parse.Cloud.useMasterKey();
}

var Migrator = function(sourceParseApp, destinationParseApp, migrationParseApp){
  this.hashs = {};
  
  this.migrationHashs = {};
  this.migrationHashs[sourceParseApp.applicationId] = {};
  this.migrationHashs[destinationParseApp.applicationId] = {};
  
  this.sourceApp = sourceParseApp;
  this.destinationApp = destinationParseApp;
  this.migrationApp = migrationParseApp;
  this.activateSourceApp();
};

Migrator.prototype.activateSourceApp = function() {
  activateParseApp(this.sourceApp);
}

Migrator.prototype.activateDestinationApp = function() {
  activateParseApp(this.destinationApp);
}
Migrator.prototype.activateMigrationApp = function() {
  activateParseApp(this.migrationApp);
}

Migrator.prototype.SOURCE_OBJECT_ID_KEY = function()
{
  return "objectId_"+this.sourceApp.applicationId;
}

Migrator.prototype.DESTINATION_OBJECT_ID_KEY = function()
{
  return "objectId_"+this.destinationApp.applicationId;
}

Migrator.prototype.OBJECT_CLASSNAME_KEY = function() 
{
  return "object_className";  
}

Migrator.prototype.collectPointer = function(object, key) 
{
  var self = this;
  var pointedObject = object.get(key);
  var destinationPointedObject = this.destinationObjectForObject(object.get(key));
  var hash = hashObject(pointedObject);
  var kv = {};
  if (!destinationPointedObject) {
    self.activateSourceApp();
    return pointedObject.fetch().then(function(o){
      return self.migrateObject(o).then(function(migratedObject){
        kv[key] = migratedObject;
        return Parse.Promise.as(kv);
      });
    }).fail(function(err){
      console.error(err, pointedObject);
      return Parse.Promise.as({});
    })
  }else{
    kv[key] = destinationPointedObject;
    return Parse.Promise.as(kv);
  }
}

Migrator.prototype.collectRelation = function(object, key)
{
  var self = this;
  var objects = [];

  var destinationObject = this.destinationObjectForObject(object);

  self.activateSourceApp();

  // Map each objects of the relation 
  return object.relation(key).query().each(function(o){
    // Migrate the object (if needed)
    return self.migrateObject(o).then(function(r){
      objects.push(r);
    });
  
  }).then(function(){

    self.activateDestinationApp();
    destinationObject.relation(key).add(objects);
    self.activateSourceApp();
    return {};

  });
}

Migrator.prototype.migrateClass = function(className)
{
  var self = this;
  this.activateSourceApp();
  var q = new Parse.Query(className);
  return q.each(function(object){
    return self.migrateObject(object);
  }).then(function(){
    self.activateSourceApp();
  })
}


Migrator.prototype.migrateObject = function(object, options)
{
  var self = this;

  this.hashs[hashObject(object)] = object;
  
  console.log("\n", "-- Exporting", object.className, object.id);
  
  options = options || {};
  
  var jsonObject = object.toJSON();
  var keys = Object.keys(jsonObject);
  var migratedObject;
  
  return this.migrate(object).then(function(_migratedObject){
    
    migratedObject = _migratedObject;
    var promises = keys.map(function(key, index){
      
      var value = jsonObject[key];
      var promise;

      var type = "property";
      if (_.isObject(value)) {

        if (value.__type == "Pointer") {
          type = value.__type;
          promise = self.collectPointer(object, key);
        } else if (value.__type == "Relation") {
          type = value.__type;
          promise = self.collectRelation(object, key);
        } else if (value.__type == "File") {
          var kv = {};
          kv[key] = value;
          promise = Parse.Promise.as(kv);
        } else {
          type = "object";
        }
      } 

      if (_.isArray(value)) {
        type = "array";
      };

      if (_.isNumber(value)) {
        type = "number";
      };

      // Swap the value for a real ACL
      if (key == "ACL") {
        type = "ACL";
        var jsonACL = value;
        var ACL = new Parse.ACL();
        ACL.permissionsById = jsonACL;
        value = ACL;
      };

      // Ignore ObjectID and others
      if (!promise && ["objectId", "updatedAt", "createdAt"].indexOf(key) == -1) {
        var kv = {};
        kv[key] = value;
        promise = Parse.Promise.as(kv);
      } else {
        type = "ignored";
      }

      if(!promise) 
      {
        promise = new Parse.Promise.as();
      }
      console.log("+ ", key, "\t\t\t\t",type);

      return promise;
    })//.tap(Parse.Promise.when).value();

    return Parse.Promise.when(promises);
    //return Parse.Promise.when(promises);

  }).then(function(){
    // Arguments have the form [{k: v}, {k: v}, {k, v}]
    var result = _.reduce(arguments, function(memo, obj) {
      return _.extend(memo, obj);
    }, {});

    self.activateDestinationApp();
    if (self.beforeDestinationSave) {
      self.beforeDestinationSave(migratedObject, result);
    } else  {
      migratedObject.set(result);
    }
    
    //return result;
    return migratedObject.save().then(function(){
      self.activateSourceApp();
      return Parse.Promise.as(migratedObject);
    })
  })

  
}

Migrator.prototype.findOrCreateDestinationId = function(object) 
{
  var self = this;
  var sourceId = object.id;
  var className = object.className;

  self.activateMigrationApp();

  var q = new Parse.Query("MigratedObject");
  q.equalTo(self.OBJECT_CLASSNAME_KEY(), object.className);
  q.equalTo(self.SOURCE_OBJECT_ID_KEY(), object.id);
  q.exists(self.DESTINATION_OBJECT_ID_KEY());

  return q.first().then(function(migrationObject){
    if (migrationObject) {
      console.log("-- Found migration object (", migrationObject.id, ") for ", object.className, object.id);
      return Parse.Promise.as(migrationObject);
    }else{
      return Parse.Promise.error();
    }
  }).fail(function(){
    console.log("-- Creating a new migration object for", object.className,object.id);
    // On the destination DB
    self.activateDestinationApp();
    
    // Save a placeholder object to generate an objectId
    var o =  (new Parse.Object(object.className))
    if (self.beforeCreatePlaceholder) {
      self.beforeCreatePlaceholder(object, o);
    };
    

    return o
      .save()
      .then(function(otherObject){

      // On the migration DB
      self.activateMigrationApp();
      
      var destinationId = otherObject.id;

      var migratedObject = new Parse.Object("MigratedObject");
      migratedObject.set(self.SOURCE_OBJECT_ID_KEY(), sourceId);
      migratedObject.set(self.DESTINATION_OBJECT_ID_KEY(), destinationId);
      migratedObject.set(self.OBJECT_CLASSNAME_KEY(), className);
      return migratedObject.save();
    });

  }).then(function(migratedObject){
    self.activateSourceApp();
    return Parse.Promise.as(migratedObject.get(self.DESTINATION_OBJECT_ID_KEY()));
  });
}

Migrator.prototype.migrate = function(object)
{
  var self = this;
  
  var className = object.className;
  var sourceId = object.id;
  return this.findOrCreateDestinationId(object).then(function(destinationId){
    
    self.activateDestinationApp();
    // We have an existing object.. let's use it
    var q = new Parse.Query(className);
    return q.get(destinationId);
  
  }).then(function(destinationObject){
    
    // Cache the destination object for reuse
    self.cacheDestinationObject(destinationObject, object);
    self.activateSourceApp();
    return Parse.Promise.as(destinationObject);

  })
}

Migrator.prototype.cacheDestinationObject = function(destinationObject, sourceObject) 
{

  var self = this;
  var srcHash = hashObject(sourceObject);
  var dstHash = hashObject(destinationObject);
  var o = {}
  o[self.sourceApp.applicationId] = sourceObject;
  o[self.destinationApp.applicationId] = destinationObject;

  self.migrationHashs[self.sourceApp.applicationId][srcHash] = o;
  self.migrationHashs[self.destinationApp.applicationId][dstHash] = o;
}

Migrator.prototype.destinationObjectForObject = function(object) 
{
  var self = this;
  var hash = hashObject(object);
  if (self.migrationHashs[self.sourceApp.applicationId][hash]) {
    return self.migrationHashs[self.sourceApp.applicationId][hash][self.destinationApp.applicationId];
  } else if (self.migrationHashs[self.destinationApp.applicationId][hash]) {
    return self.migrationHashs[self.destinationApp.applicationId][hash][self.destinationApp.applicationId];
  }
}


module.exports = Migrator;