/* global Promise */
'use strict';

var mysql = require('mysql');
var fs = require('fs');


function PersistenceService() {
    this.pool = null;
}

PersistenceService.prototype.configure = function(mysqlPoolConfig){
    this.pool = mysql.createPool(mysqlPoolConfig);
}

PersistenceService.prototype.ensureVersion = function(scriptsPath){
    var that = this;
                    
    return that.withConnection(function(connection){
        
        return Promise.all([
            new Promise(function(resolve,reject){   
                 
                if(!fs.existsSync(scriptsPath))
                    return fs.mkdir(scriptsPath);
                
                fs.readdir(scriptsPath,function(err,files){
                    if(err){
                        return process.nextTick(function(){
                            reject(err);
                        });
                    }
                    
                    files.sort(function(a,b){
                        return a < b ? -1 : 1;
                    });
                    
                    return process.nextTick(function(){
                        resolve(files); 
                    });
                });
            }),
            that.queryPromise(connection,"CREATE TABLE IF NOT EXISTS __ScriptHistory(SCRIPT_NAME VARCHAR(300) NOT NULL,SCRIPT_CONTENT TEXT, SCRIPT_DATE TIMESTAMP)").then(function(){
                return that.queryPromise(connection,"SELECT MAX(SCRIPT_NAME) AS LastScript FROM __ScriptHistory");    
            })            
        ]).then(function(rets){
            var files = rets[0];
            var query  = rets[1];
            
            var lastFile = query.rows[0].LastScript || '';
            
            // log.info('Verificando ',files.length,' arquivos para LastFile ',lastFile);
            
            files = files.filter(function(file){
               return file > lastFile; 
            });
            
            if(!files.length)
                return Promise.resolve(0);
            
            return new Promise(function(resolve,reject){
                var i = 0;
                var fnRunItem = function(file){
                    if(lastFile >= file)
                        return;
                
                    // log.info('Executando ',file);
                    
                    var script = fs.readFileSync(scriptsPath + '/' + file,'utf8');

                    return that.queryPromise(connection, script).then(function(){
                        i++;
                        
                        // log.info('Executado!');
                        
                        
                        that.queryPromise(connection,'INSERT INTO __ScriptHistory (SCRIPT_NAME,SCRIPT_CONTENT,SCRIPT_DATE) VALUES (?,?,NOW())',[file,script,]).then(function(){
                            if(i >= files.length){
                                return process.nextTick(function(){
                                    // log.info('resolving');
                                    resolve(); 
                                });
                            }
                            
                            process.nextTick(function(){
                                fnRunItem(files[i]);
                            }); 
                        });
                    },function(err){
                        // log.error('Falha rodando ',file,err);
                        process.nextTick(function(){
                            reject(err);
                        });
                    });
                };
                
                fnRunItem(files[i]);
            });

        });     
    });
}

PersistenceService.prototype.extendConnection = function(connection){
    
}

PersistenceService.prototype.acquireConnection = function () {
    var that = this;
    
    return new Promise(function(resolve,reject){
        that.pool.getConnection(function(err,connection){
            if(err){
                // log.error('Falha conectando',err);
                return process.nextTick(function(){
                    reject(err); 
                });
            } 
           
            if(!connection.$extended)
                that.extendConnection(connection);

            process.nextTick(function(){
                resolve(connection); 
            });
        });        
    });
}

PersistenceService.prototype.releaseConnection = function(connection){
    connection.release();
}

PersistenceService.prototype.withConnection = function(fn){
    var that = this;
    
    return that.acquireConnection().then(function(connection){
        
        try{
            var ret = fn(connection);
        }catch(err){
            // log.error('Falha invocando withConnection Fn',err);
        }
        
        if(ret instanceof Promise){
            return ret.then(function(){
               connection.release();
            });
        }
        
        connection.release();
        return Promise.resolve(0);
    });
}

PersistenceService.prototype.queryPromise = function (){
    var connection = arguments[0];
    
    var args = new Array(arguments.length-1);
    for(var i = 1 ; i < arguments.length;i++){
        args[i-1] = arguments[i];
    }
    
    return new Promise(function(resolve,reject){
        
        args.push(function(error,results,fields){
            if(error){
                return process.nextTick(function(){
                    reject(error); 
                });
            }
                    
            return process.nextTick(function(){
                resolve({
                    rows: results,
                    fields: fields
                }); 
            });
        });
        
        connection.query.apply(connection, args);
        args = null;
        connection = null;
    });
}

module.exports = new PersistenceService();