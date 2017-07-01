const MongoDb = require('../lib/db.js');

var monDb = '';


/**
 *  
 *  封装mongodb的增、删、改、查
 *  Date :2017-5-25
 */

var sdbService = {};

sdbService.query = function(collectionName,where,dbName){
    return new Promise(function(resolve,reject){
        _openDb(collectionName,dbName).then(function(obj){
            obj.collection.find(where).toArray(function(err, docs) {
                    if(err){
                        concole.log('document find err' + err);
                         obj.db.close();
                        reject(err);
                    } 
                         
                    resolve(docs);
                    obj.db.close();
            });
       }).catch(function(err){
              reject(err);
          });
    });
    

}

sdbService.insert = function(collectionName,data,dbName){
    data._id = monDb.getIdStr();
    return new Promise(function(resolve,reject){
        _openDb(collectionName,dbName).then(function(obj){
              obj.collection.insert(data,function(err,result){
                if(err){
                    concole.log('document insert err' + err);
                     obj.db.close();
                    reject(err);
                } 
                resolve(result.insertedIds[0]);
                obj.db.close();
            });  
        }).catch(function(err){
              reject(err);
          });
    });
   
}

sdbService.update = function(collectionName,id,data,dbName){
    return new Promise(function(resolve,reject){
         _openDb(collectionName,dbName).then(function(obj){
             obj.collection.updateOne({ _id : id}, { $set: data }, function(err, result) {
                    if(err){
                         console.log('document update err' + err);
                          obj.db.close();
                         reject(err);  
                     } 
                    resolve(result.result.ok);
                    obj.db.close();
                });
         }).catch(function(err){
              reject(err);
          });
    });

}


sdbService.remove = function(collectionName,id,dbName){
    return new Promise(function(resolve,reject){
          _openDb(collectionName,dbName).then(function(obj){
               obj.collection.deleteOne({ _id :id }, function(err, result) {
                    if(err){
                         concole.log('document remove err' + err);
                         obj.db.close();
                         reject(err); 
                     } 
                    resolve(result.result.ok);
                    obj.db.close();
                });
          }).catch(function(err){
              reject(err);
          });
    });

}

sdbService.saveFile = function(data,metadata){
     return new Promise(function(resolve,reject){
         let db = monDb.getFileDb();
         db.open(function(err, db) {
             if(err){
               reject(err);  
               return;
             }
            var fileId = monDb.getIdStr();
            console.log(fileId);
            var gridStore = new monDb.GridStore(db, fileId, 'w',{root:'file',metadata:metadata});
             gridStore.open(function(err, gridStore) {
                  if(err){
                     reject(err);
                      return;  
                  }
                gridStore.write(data, function(err, gridStore) { 
                     if(err){
                        reject(err);
                        return;  
                     }
                    gridStore.close(function(err, result) {
                        if(err){
                             reject(err);
                             return;  
                        }
                        resolve(result.filename);
                        db.close();
                    });

                });

             });

         });


     });
}

sdbService.readFile = function(fileId){
     return new Promise(function(resolve,reject){
         let db = monDb.getFileDb();
         db.open(function(err, db) { 
             if(err){
                     reject(err);
                      return;  
                  }  
           // var gs2 = new GridStore(db, "test", "r");
            //let fileId = '592fd4536e27772c8c318dd3';
            var gridStore = new monDb.GridStore(db, fileId, 'r',{root:'file'});
            // var stream = gridStore.stream(true);
            // resolve(stream);
            // db.close();
             gridStore.open(function(err, gridStore) {
                 if(err){
                      reject(err);
                      return;  
                  }
                gridStore.seek(0, function() {
                    gridStore.read(function(err, contet) {
                        if(err){
                           reject(err);
                           return;  
                        }
                        resolve(contet);
                        db.close();
                    });
                });

             });

         });

     });
}



function  _openDb(collectionName,dbName){
        return new Promise(function(resolve,reject){
           let db =  _getUseDb(dbName);
           db.open(function(err, db) {
               if(err){
                    reject(err);
                    return console.log('db open err' + err);
                }
               let collection = db.collection(collectionName);
               _obj ={db:db,collection:collection}; 
               resolve(_obj);
           });

      });
      
   }

function _getUseDb(dbName){
    var db = monDb.getDefaultDb();
    if(dbName && typeof dbName =='string'){
        db = monDb.getDb(dbName);
    }
    return db;
}


// var serivce ={
//     sdbService:sdbService
// } 

/**
 * 导出插件
 */
exports.register = function (server, options, next) {
   // monDb = new  MongoDb(options.db_mongo);
    monDb = new  MongoDb(options);
    server.expose('service', sdbService);
    server.log('info','completed to light-api-mongo plugin init');
    return  next();
};

exports.register.attributes = {
    pkg: require('../package.json')
};

