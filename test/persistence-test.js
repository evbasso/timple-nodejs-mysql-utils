var assert = require('assert');

var persistence = require('../index');

describe("Criar conexão",function(){
	
	it('configura',function(){
		persistence.configure({
			host: 'localhost',
            user: 'root',
            password: 'root',
			database: 'test'			
		});
	})
	
	it("obter conexao",function(){
		return persistence.withConnection(function(connection){
			
		});
	});
	
	it('garante versao',function(){
		return persistence.ensureVersion(__dirname + '/db-scripts');
	});
});