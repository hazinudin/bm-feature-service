import pika, sys, os, json, redis
import api.nat_bridge_api as bridge_api


QUEUE_NAME='bridge-master-data'
RMQ_HOST = 'localhost'
RMQ_PORT = 5672

REDIS_HOST = RMQ_HOST
REDIS_PORT = 6379

def main():
    # RabbitMQ connection
    rmq_conn = pika.BlockingConnection(pika.ConnectionParameters(host=RMQ_HOST, port=RMQ_PORT))
    channel = rmq_conn.channel()

    # Redis connection
    redis_client = redis.StrictRedis(host=RMQ_HOST, port=REDIS_PORT, db=0)

    def callback(ch, method, properties, body):
        try:
            input_d = json.loads(body)
            pid = input_d['pid']  # Get the PID
            jobid = f"{pid}"
            input_method = input_d['method']  # Get the method
            feature = input_d['feature']  # Get the feature

            if str(input_method).lower() == 'get_oid':
                oid = bridge_api.get_active_oids([feature['BRIDGE_ID']])
                result = {'objectid': int(oid)}
            
            elif str(input_method).lower() == 'insert':
                result =bridge_api.insert([feature])
            
            elif str(input_method).lower() == 'delete':
                oid = bridge_api.get_active_oids([feature['BRIDGE_ID']])
                result = bridge_api.delete([oid])
            
            elif str(input_method).lower() == 'update':
                result = bridge_api.update(feature)

            else:
                result = {"error": "Unsupported method."}
            
            # Write result to redis
            redis_client.publish(jobid, json.dumps(result))

        except KeyError:
            result = {"error": "Missing keys in body. {}".format(input_d.keys())}
            redis_client.publish(jobid, json.dumps(result))

        except:
            result = {"error": "Unhandled error. {}".format(str(input_d))}
            redis_client.publish(jobid, json.dumps(result))

    tag = channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

    print("Tag: {}".format(tag))
    print(' [*] Listening at port {}. Waiting for messages. To exit press CTRL+C'.format(RMQ_PORT))
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
