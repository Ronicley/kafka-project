import { Kafka, Partitioners } from 'kafkajs';
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';

const registry = new SchemaRegistry({ host: 'http://localhost:8081' });
const kafka = new Kafka({
	brokers: ['localhost:9092'],  // Use a porta externa
	clientId: 'my-producer',     // Adicione um client ID
});

const userSchema = {
	type: 'record',
	name: 'User',
	namespace: 'com.meudominio.models',
	fields: [
		{ name: 'email', type: 'string' },
		{ name: 'name', type: 'string' },
		{ name: 'age', type: 'int' }
	]
};

async function produce() {
	const producer = kafka.producer({
		createPartitioner: Partitioners.LegacyPartitioner,
		allowAutoTopicCreation: true,  // Permite criação de tópicos
	});
	
	await producer.connect();
	
	try {
		const { id } = await registry.register({
			type: SchemaType.AVRO,
			schema: JSON.stringify(userSchema)
		}, {
			subject: 'users-value'
		});
		
		const message = {
			email: 'test@corrigido.com',
			name: 'Fulano',
			age: 30
		};
		
		const encodedValue = await registry.encode(id, message);
		
		await producer.send({
			topic: 'users',
			messages: [{ value: encodedValue }]
		});
		
		console.log('✅ Mensagem enviada com sucesso!');
	} catch (err) {
		console.error('Erro no producer:', err);
	} finally {
		await producer.disconnect();
	}
}

// Adicione um delay para garantir que o Kafka esteja pronto
setInterval(() => {
	produce().catch(err => console.error('Erro global:', err));
}, 10000);  // Espera 10 segundos antes de iniciar
