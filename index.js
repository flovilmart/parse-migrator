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
	console.log("---- ACTIVATE SOURCE APP ----");
	activateParseApp(this.sourceApp);
}

Migrator.prototype.activateDestinationApp = function() {
	console.log("---- ACTIVATE DESTINATION APP ----");
	activateParseApp(this.destinationApp);
}
Migrator.prototype.activateMigrationApp = function() {
	console.log("---- ACTIVATE MIGRATION APP ----");
	activateParseApp(this.migrationApp);
}

Migrator.prototype.collectPointer = function(object, key) 
{
	var self = this;
	var pointedObject = object.get(key);
	var destinationPointedObject = this.destinationObjectForObject(object.get(key));
	var hash = hashObject(pointedObject);
	var kv = {};
	console.log("Collecting pointer on key",key,hash);
	console.log(destinationPointedObject);
	if (!destinationPointedObject) {
		self.activateSourceApp();
		return pointedObject.fetch().then(function(o){
			return self.migrateObject(o).then(function(migratedObject){
				kv[key] = migratedObject;
				return Parse.Promise.as(kv);
			});
		}).fail(function(err){
			console.error(err);
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
	console.log(destinationObject);
	self.activateSourceApp();
	return object.relation(key).query().each(function(o){
		return self.migrateObject(o).then(function(r){
			objects.push(r);
			self.cacheDestinationObject(r, o);
		});
	}).then(function(){
		self.activateDestinationApp();
		destinationObject.relation(key).add(objects);
		return destinationObject.save();
	}).then(function(){
		self.activateSourceApp();
		return {};
	})
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
	
	console.log("Exporting "+hashObject(object));
	
	options = options || {};
	
	var jsonObject = object.toJSON();
	var keys = Object.keys(jsonObject);
	var migratedObject;
	
	return this.migrate(object).then(function(_migratedObject){
		migratedObject = _migratedObject;
		var promises = keys.map(function(key, index){
			var value = jsonObject[key];
			if (_.isObject(value)) {
				if (value.__type == "Pointer") {
					return self.collectPointer(object, key);
				} else if (value.__type == "Relation") {
					return self.collectRelation(object, key);
				}
			} 
			if (key == "ACL") {
				var jsonACL = value;
				var ACL = new Parse.ACL();
				ACL.permissionsById = jsonACL;
				value = ACL;
			};
			var kv = {};
			kv[key] = value;
			return Parse.Promise.as(kv);
		});
		return Parse.Promise.when(promises);
	}).then(function(){
		
		var result = _.reduce(arguments, function(memo, obj) {
			return _.extend(memo, obj);
		}, {});

		delete result.objectId;
		delete result.updatedAt;
		delete result.createdAt;

		self.activateDestinationApp();
		migratedObject.set(result);
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

	self.activateMigrationApp();

	var q = new Parse.Query("MigratedObject");
	q.equalTo("object_className", object.className);
	q.equalTo(self.sourceApp.applicationId+"_objectId", object.id);
	q.exists(self.destinationApp.applicationId+"_objectId");

	return q.first().then(function(object){
		
		if (object) {
			return Parse.Promise.as(object);
		}else{
			console.log("Not Found");
			return Parse.Promise.error();
		}
	
	}).fail(function(){
		// On the destination DB
		self.activateDestinationApp();
		// Save a placeholder object to generate an objectId
		var toSaveObject = new Parse.Object(object.className);
		
		return toSaveObject.save().then(function(otherObject){

			// On the migration DB
			self.activateMigrationApp();
			
			var migratedObject = new Parse.Object("MigratedObject");
			var migratedObjectJSON = {
				object_className: object.className,
			};
			migratedObjectJSON[self.sourceApp.applicationId+"_objectId"] = sourceId;
			migratedObjectJSON[self.destinationApp.applicationId+"_objectId"] = otherObject.id;

			return migratedObject.save(migratedObjectJSON);

		});

	}).then(function(migratedObject){
		self.activateSourceApp();
		return Parse.Promise.as(migratedObject.get(self.destinationApp.applicationId+"_objectId"));
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

	});
}

Migrator.prototype.cacheDestinationObject = function(destinationObject, sourceObject) 
{

	var self = this;
	var srcHash = hashObject(sourceObject);
	var dstHash = hashObject(destinationObject);
	console.log("Caching ", srcHash, dstHash);
	
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