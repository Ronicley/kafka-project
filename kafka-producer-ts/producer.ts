import { Kafka, Partitioners} from "kafkajs";

const kafka = new Kafka({
	clientId: 'node-js-producer',
	brokers: ['localhost:9092']
});

const producer = kafka.producer({
	createPartitioner: Partitioners.LegacyPartitioner
});

const topic = 'cross-platform-topic';

async function run() {
	await producer.connect();
	let count = 0;
	setInterval(async () => {
		try {
			const message = `----------Mensagem em ${new Date().toISOString()}`;
			await producer.send({
				topic,
				messages: [
					{
						key: `key-${count}`,
						value: message
					}
				]
			});
			count++;
			console.log(`++++++Mensagem enviada ${message} key-${count}`);
		} catch (err){
			console.error('Erro no producer:', err);
			
		}
	}, 2000);
}

run().catch(console.error);
