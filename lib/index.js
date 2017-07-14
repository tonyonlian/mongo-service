const MongoDb = require('../lib/db.js');
const  async =  require('async');

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

/**
 * 分页查询
 * @param {*} collectionName ：集合名
 * @param {*} pageParam ：分页参数 eg {pageNo:2,pageSize:10}
 * @param {*} where ：查询条件
 * @param {*} sort ：排序条件
 * @param {*} dbName ：monodb的数据库名
 * 
 */
sdbService.queryListPage = function(collectionName, where, sort, pageParam,dbName){
    return new Promise(function(resolve,reject){
        _openDb(collectionName,dbName).then(function(obj){
            async.series([
                function(callback){ 
                    obj.collection.count(where,callback); 
                },
                function(callback){
                    obj.collection.find(where,{skip:(pageParam.pageNo - 1) * pageParam.pageSize,limit: pageParam.pageSize}).sort(sort).toArray(callback);
                }
            ],function(err,result){
                if(err){
                    concole.log('document find err' + err);
                    obj.db.close();
                    return reject(err);
                }

                let _obj = {
                        pageSize:pageParam.pageSize,
                        pageNo:pageParam.pageNo,
                        count:result[0]
                        }
                result[0] = _obj;
                resolve(result);
                obj.db.close();

            });
   
       }).catch(function(err){
              reject(err);
          });
    });
}

/**
 * 查询指定条数的数据
 * @param {*} collectionName ：集合名
 * @param {*} limit ：限制参数{skip:2,limit: 10}
 * @param {*} where ：查询条件
 * @param {*} sort ：排序条件{field:-1} -1降序 1升序
 * @param {*} dbName ：monodb的数据库名
 * 
 */

sdbService.queryByLimtCount = function(collectionName, where, sort, limit,dbName){
     return new Promise(function(resolve,reject){
          _openDb(collectionName,dbName).then(function(obj){
            if(sort){//有排序条件
                 obj.collection.find(where,limit)
                 .sort(sort)
                 .toArray(function(err,result){
                    if(err){
                        concole.log('document find err' + err);
                        obj.db.close();
                        return reject(err);
                    }
                    resolve(result);
                    obj.db.close();
                 });
            }else{//没有排序条件
                obj.collection.find(where,limit)
                 .sort(sort)
                 .toArray(function(err,result){
                     if(err){
                        concole.log('document find err' + err);
                        obj.db.close();
                        return reject(err);
                     }
                     resolve(result);
                     obj.db.close();
                 });
            }
          }).catch(function(err){
              reject(err);
          });
        
    })

}

/**
 * 查询总数
 * @param {*} collectionName ：集合名
 * @param {*} where ：查询条件
 * @param {*} dbName ：monodb的数据库名
 * 
 */

sdbService.getCount = function(collectionName, where,dbName){
    return new Promise(function(resolve,reject){
        _openDb(collectionName,dbName).then(function(obj){
             obj.collection.count(where,function(err,count){
                if(err){
                     console.log('query count err :'+err);
                     obj.db.close();
                     return reject(err);
                }
                resolve({count:count});
                obj.db.close();
             }); 
        }).catch(function(err){
            reject(err);
        })
    });
}

/**
 * 根据查询条件，删除多个文档
 * @param {*} collectionName ：集合名
 * @param {*} where ：删除的查询条件
 * @param {*} dbName ：monodb的数据库名
 * 
 */

sdbService.removeAll = function(collectionName, where,dbName){
    return new Promise(function(resolve,reject){
        _openDb(collectionName,dbName).then(function(obj){
             obj.collection.remove(where, {w:1}, function(err, result) {
                if(err){
                     console.log('remove doc err:'+err);
                     obj.db.close();
                     return reject(err);
                }
                resolve({numberOfRemovedDocs:result.result.n});
                obj.db.close();
            });
        }).catch(function(err){
            reject(err);
        })

    })
}

sdbService.saveFile = function(data,metadata,filename){
     return new Promise(function(resolve,reject){
         let db = monDb.getFileDb();
         db.open(function(err, db) {
             if(err){
               reject(err);  
               return;
             }
            var fileId = monDb.getObjectID();
            console.log(fileId);
            var gridStore = new monDb.GridStore(db, fileId,filename, 'w',{root:'file',metadata:metadata});
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
                        resolve(result._id);
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
        
            var gridStore = new monDb.GridStore(db, monDb.toObjectID(fileId), 'r',{root:'file'});
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

/**
 * 删除文件
 * @param {*} fileId ：文件id
 * 
 */
sdbService.deleteFile = function(fileId){
    return new Promise(function(resolve,reject){
        let db = monDb.getFileDb();
         db.open(function(err, db) { 
             if(err){
                     reject(err);
                      return;  
                  }
            monDb.GridStore.unlink(db, monDb.toObjectID(fileId),{root:'file'} ,function(err, gridStore){
                if(err){
                    return reject(err);
                }
                resolve('ok');
                db.close();
                
            });
         }); 
    });
}

/**
 * 获取文件的元数据
 * @param {*} fileId ：文件id 
 */

sdbService.getFileMetadata  = function(fileId){
    return new Promise(function(resolve,reject){
        let db = monDb.getFileDb();
         db.open(function(err, db) { 
             if(err){
                return reject(err);;  
            }
            db.collection('file.files', function(err, collection) {
                if(err){
                    return reject(err);
                }
                collection.find({_id:monDb.toObjectID(fileId)}).toArray(function(err, docs) {
                    if(err){
                        concole.log('document find err' + err);
                        db.close();
                        return reject(err);
                    }    
                    resolve(docs);
                    db.close();
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

