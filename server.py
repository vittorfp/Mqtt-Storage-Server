from pymongo import MongoClient
import paho.mqtt.client as mqtt
import json

mqttBroker_ip = '127.0.0.1';
mqttBroker_port = 1883;

mongo_ip = 'localhost';
mongo_port = 27017;

# callback chamada quando o servidor consegue se conectar ao broker
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # Sobrescreve a todos os topicos
    client.subscribe("#")

# callback chamada quando o chega alguma mensagem
def on_message(client, userdata, msg):
    # Aqui o servidor pega o que ele recebeu e taca no mongo
    print( msg.topic+" "+str(msg.payload))

    entrada = json.loads( str(msg.payload) )
    table.insert(entrada)

def setupMQTT():
    # Constroi um cliente MQTT
    mqttClient = mqtt.Client()

    # Define as callbacks
    mqttClient.on_connect = on_connect
    mqttClient.on_message = on_message

    # Conecta e fica rodando infinitamente
    mqttClient.connect(mqttBroker_ip, mqttBroker_port , 60)
    mqttClient.loop_forever()


clienteMongo = MongoClient(mongo_ip, mongo_port)
db = clienteMongo.registros
table = db.valores

setupMQTT()
