const MongoDb =  require('mongodb');
const Db = MongoDb.Db;
const Connection = MongoDb.Connection;
const Server = MongoDb.Server;
const ReplSet = MongoDb.ReplSet;
const Mongos = MongoDb.Mongos;
const ObjectID = MongoDb.ObjectID;
const GridStore = MongoDb.GridStore;
const Grid = MongoDb.Grid;


// var serverArr = [];
// servers.forEach(function(server) {
//    var _tempServer = new Server(server.host, server.port,{ auto_reconnect: true });
//    serverArr.push(_tempServer);
//     console.log(server);
// });


function MonDb(options){
    servers = []
    options.servers.forEach(function(server) {
        _tempArr = server.split(":");
        _objServer = {host:_tempArr[0],port:_tempArr[0]};
        servers.push(_objServer);
    });

    this._otherDB = options.otherDB;
    this._defaultDB = options.defaultDB;
    this._servers = servers;
    this._host = servers[0].host;
    this._port = servers[0].port;
    // this._servers = options.servers;
    // this._host = options.servers[0].host;
    // this._port = options.servers[0].port;
    this.GridStore = GridStore;
    this.defaut_db = new Db(this._defaultDB,new Server( this._host ,this._port,{auto_reconnect:true,poolSize:1},{w:1}));
}

MonDb.prototype.getDb =function(dbName){
     var _this = this;
   if(OTHER_DB.indexOf(dbName)>-1){
       return new Db(dbName,new Server(  _this._host, _this._port,{auto_reconnect:true,poolSize:1},{w:1}));
   }else{
       throw new Error('the dbName of MongoDb :'+ dbName + ' no exist!');
   }
}

MonDb.prototype.getDefaultDb = function(){
     var _this = this;
     return  _this.defaut_db;
}
MonDb.prototype.getIdStr = function(){
     return  new ObjectID().toHexString();;
}

MonDb.prototype.getFileDb = function(){
     var _this = this;
     return new Db('service_system',new Server(  _this._host, _this._port,{auto_reconnect:true,poolSize:1},{w:1}));
}



module.exports = MonDb;